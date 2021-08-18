import paho.mqtt.client as mqtt
# import some cyberpunk / eurobeat band names
import sys, traceback, os, subprocess, time, json, signal, inspect
from daemon import Daemon
from dataclasses import dataclass
from dataclasses_json import dataclass_json
from typing import *
from multitimer import MultiTimer
import minimalmodbus

# This is some kind of fan controller targeted at the invertek e3
# I guess it's a sort of MQTT bridge?

class HFCDaemon(Daemon):
    def run(self):
        h_fanController = HFC()
        my_path = os.path.dirname(os.path.abspath(__file__))
        config = open(my_path + "/hfc_config.json", "r")
        h_fanController.data = HFC.data.from_json(config.read())
        config.close()

        h_fanController.run()


@dataclass_json
@dataclass
class Modbus_Checkup:
    name: str
    fmt: str
    register: int


class HFC(mqtt.Client):
    """Controls the fan on the roof"""

    version = '2020'

    @dataclass_json
    @dataclass
    class data:
        name: str
        description: str
        default_speed: int
        boot_check_list: Dict[str, List[str]]
        long_checkup_freq: int
        long_checkup_leng: int
        modbus_checkups: Dict[str, int]
        modbus_port: str
        modbus_address: int
        modbus_baud: int
        modbus_timeout: float
        modbus_tries: int
        mqtt_broker: str
        mqtt_port: int
        mqtt_timeout: int

    def lineno(self):
        return inspect.currentframe().f_back.f_lineno

    # overloaded MQTT funcitons from (mqtt.Client)
    def on_log(self, client, userdata, level, buff):
        if level != mqtt.MQTT_LOG_DEBUG:
            print(level)
            print(buff)
            sys.stdout.flush()
        if level == mqtt.MQTT_LOG_ERR:
            print ("ERROR")
            traceback.print_exc()
            sys.stdout.flush()
            self.running = False;

    def on_connect(self, client, userdata, flags, rc):
        self.subscribe("reporter/checkup_req")
        self.subscribe("display/drive_run")
        self.subscribe("display/drive_reset")
        self.subscribe("display/drive_speed")
        print("Connected: " + str(rc))
        sys.stdout.flush()

    def on_disconnect(self, client, userdata, rc):
        print("Disconnected: " + str(rc))
        sys.stdout.flush()
        self.running = False

    def on_message(self, client, userdata, message):
        if message.topic == "reporter/checkup_req":
            print("Checkup received.")
            sys.stdout.flush()
            self.checkup = True
            if (self.timer == None or self.timer._timer == None or self.timer._timer.isAlive() == False):
                print("Timer invalid, starting new timer")
                sys.stdout.flush()
                self.timer = MultiTimer(interval=1, function = self.renew)
                self.timer.start();

        if message.topic == "display/drive_speed":
            print("New Speed Received: ", end='')
            decoded = message.payload.decode('utf-8')
            print(decoded)
            sys.stdout.flush()
            try:
                received_speed = int(decoded)
                if (received_speed > self.max_speed):
                    raise ValueError("New speed above max speed")

                if (received_speed < -self.max_speed):
                    raise ValueError("New speed below minimum reverse speed")
                
                self.new_speed = True
                self.speed = received_speed
            except ValueError as err:
                print(err)
                print("Error converting string \"" + decoded + "\" to for speed")
                sys.stdout.flush()

        if message.topic == "display/drive_run":
            print("Run command received: ", end='')
            decoded = message.payload.decode('utf-8')
            print(decoded)
            sys.stdout.flush()
            if (decoded.lower() == "true" or decoded == "1"):
                self.drive_run = True
            else:
                self.drive_run = False

        if message.topic == "display/drive_reset":
            print("Reset received.")
            sys.stdout.flush()
            self.drive_reset = True

    def bootup(self):
        self.pings = 0

        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        self.running = True
        self.exiting = False
        self.checkup = False
        self.drive_run = False
        self.drive_reset = False
        self.new_speed = False

        for try_ in range(self.data.modbus_tries):
            try:
                status_word = self.instr.read_register(5)
                self.max_speed = (int)(self.instr.read_register(128)/5)
                self.speed = (int)(self.instr.read_register(1, signed=True))
                self.drive_ready = bool(status_word & (1<<6))
                self.drive_tripped = bool(status_word & (1<<1))
                self.drive_running = bool(status_word & (1<<0))
                self.last_drive_running = self.drive_running
                self.drive_error = status_word >> 8
                break
            except IOError:
                print(self.lineno(), ": Failed to write to instrument, try: ", try_, "out of", self.data.modbus_tries - 1)
                sys.stdout.flush()
                if try_ == self.data.modbus_tries - 1:
                    self.running = False
                    self.exiting = True

        self.notify_bootup()

    def signal_handler(self, signum, frame):
        self.running = False
        self.exiting = True
        print("Caught a deadly signal: ", signum, " ", frame, "!")
        sys.stdout.flush()

    def notify(self, path, params, retain=False):
        params['time'] = str(time.time())
        # if (path != "running"):
        if (True):
            print(params)
            sys.stdout.flush()

        topic = self.data.name + '/' + path
        self.publish(topic, json.dumps(params), retain=retain)
        if (path != "running"):
            print("Published " + topic)
            sys.stdout.flush()

    def notify_bootup(self):
        boot_checks = {}

        boot_checks["Max_Speed"] = self.max_speed;
        boot_checks["Drive_Ready"] = self.drive_ready
        boot_checks["Drive_Tripped"] = self.drive_tripped
        boot_checks["Drive_Running"] = self.drive_running
        boot_checks["Drive_Error"] = self.drive_error

        print("Bootup:")

        for bc_name, bc_cmd in self.data.boot_check_list.items():
            boot_checks[bc_name] = subprocess.check_output(
                    bc_cmd, 
                    shell=True
            ).decode('utf-8')

        self.notify('bootup', boot_checks, retain=True)

    def renew(self):
        if self.renewing == True:
            return;
        else:
            self.renewing = True
        for try__ in range(3):
            try:
                checks = {}
                for try_ in range(self.data.modbus_tries):
                    try:
                        status_word = self.instr.read_register(5)
                        self.drive_ready = bool(status_word & (1<<6))
                        self.drive_tripped = bool(status_word & (1<<1))
                        self.drive_running = bool(status_word & (1<<0))
                        self.drive_error = status_word >> 8
                        break
                    except IOError:
                        print(self.lineno(), ": Failed to read from instrument, try: ", try_ + 1, "out of", self.data.modbus_tries)
                        sys.stdout.flush()
                        if try_ == self.data.modbus_tries - 1:
                            self.running = False
                            self.exiting = True

                if (self.checkup or self.new_speed) == True:
                    for try_ in range (self.data.modbus_tries):
                        try:
                            self.instr.write_register(1, self.speed, signed=True)
                            self.new_speed = False
                        except IOError:
                            print(self.lineno(), ": Failed to write to instrument, try: ", try_ + 1, "out of", self.data.modbus_tries)
                            sys.stdout.flush()
                            # traceback.print_exc()
                            if try_ == self.data.modbus_tries - 1:
                                self.running = False
                                self.exiting = True

                if self.checkup == True:
                    for check_name, check_register in self.data.modbus_checkups.items():
                        for try_ in range(self.data.modbus_tries):
                            try:
                                if check_name == "Max_Speed":
                                    checks[check_name] = self.instr.read_register(check_register) / 5
                                elif check_name == "Set_Point" or check_name == "Output_Frequency":
                                    checks[check_name] = self.instr.read_register(check_register, signed=True)
                                else:
                                    checks[check_name] = self.instr.read_register(check_register)
                                break
                            except IOError:
                                print(self.lineno(), ": Check name:", check_name, " Failed to read from instrument, try: ", try_ + 1, "out of", self.data.modbus_tries)
                                sys.stdout.flush()
                                # traceback.print_exc()
                                if try_ == self.data.modbus_tries - 1:
                                    self.running = False
                                    self.exiting = True

                    checks["Drive_Ready"] = self.drive_ready
                    checks["Drive_Tripped"] = self.drive_tripped
                    checks["Drive_Running"] = self.drive_running
                    checks["Drive_Error"] = self.drive_error
                    self.pings += 1
                    if(self.pings % self.data.long_checkup_freq == 0):
                        self.pings = 0
                        long_checkup_count = 0
                        # encapsulate in the trying
                        for check_name, check_command in self.data.boot_check_list.items():
                            long_checkup_count += 1
                            if long_checkup_count > self.data.long_checkup_leng:
                                break
                            checks[check_name] = subprocess.check_output(
                                    check_command,
                                    shell=True
                            ).decode('utf-8')
        
                    self.notify('checkup', checks)
                    self.checkup = False
                elif self.last_drive_running == True:
                    for check_name, check_register in self.data.modbus_checkups.items():
                        for try_ in range(self.data.modbus_tries):
                            try:
                                if check_name == "Max_Speed":
                                    checks[check_name] = self.instr.read_register(check_register) / 5
                                elif check_name == "Set_Point" or check_name == "Output_Frequency":
                                    checks[check_name] = self.instr.read_register(check_register, signed=True)
                                else:
                                    checks[check_name] = self.instr.read_register(check_register)
                                break
                            except IOError:
                                print(self.lineno(), ": Check name:", check_name, " Failed to read from instrument, try: ", try_ + 1, "out of", self.data.modbus_tries)
                                sys.stdout.flush()
                                # traceback.print_exc()
                                if try_ == self.data.modbus_tries - 1:
                                    self.running = False
                                    self.exiting = True

                    checks["Drive_Ready"] = self.drive_ready
                    checks["Drive_Tripped"] = self.drive_tripped
                    checks["Drive_Running"] = self.drive_running
                    checks["Drive_Error"] = self.drive_error
                    self.notify('running', checks)

        
                # renew the on command
                for try_ in range(self.data.modbus_tries):
                    try:
                        control_word = bool(self.drive_run and self.drive_ready) | (bool(self.drive_reset) << 2)
                        self.drive_reset = False
                        self.instr.write_register(0, control_word)
                    except IOError as err:
                        print(self.lineno(), ": Failed to write to instrument, try: ", try_ + 1, "out of", self.data.modbus_tries)
                        print("IOError: " + format(err))
                        sys.stdout.flush()
                        # traceback.print_exc()
                        if try_ == self.data.modbus_tries - 1:
                            self.running = False
                            self.exiting = True

                self.last_drive_running = self.drive_running
                break
            except:
                print(self.lineno(), ": Failed to run renewal loop, try: ", try__ + 1, "out of", 3)
                sys.stdout.flush()
                # traceback.print_exc()
                if try__ == 2:
                    self.running = False
                    self.exiting = True

        self.renewing = False

    def run(self):
        self.timer = MultiTimer(interval=1, function = self.renew)
        while True:
            self.running = True
            self.renewing = False
            while self.running:
                try:
                    self.connect(self.data.mqtt_broker, self.data.mqtt_port,
                            self.data.mqtt_timeout)
                    self.instr = minimalmodbus.Instrument(self.data.modbus_port, 
                            self.data.modbus_address)
                    self.instr.serial.baudrate = self.data.modbus_baud
                    self.instr.serial.timeout = self.data.modbus_timeout
                    print("Timeout: " + str(self.instr.serial.timeout))
                    self.bootup()
                    self.timer.start()

                    while self.running:
                        self.loop()

                    self.timer.stop()
                    self.disconnect()
                except:
                    traceback.print_exc()
                    sys.stdout.flush()
                    self.timer.stop()

                    pass

                if self.exiting:
                    exit(0)
        exit(1)
