import paho.mqtt.client as mqtt
# import some cyberpunk / eurobeat band names
import traceback, os, subprocess, time, json, signal
from daemon import Daemon
from dataclasses import dataclass
from dataclasses_json import dataclass_json
from typing import *
from multitimer import MultiTimer
import minimalmodbus

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
        boot_check_list: Dict[str, List[str]]
        long_checkup_freq: int
        long_checkup_leng: int
        modbus_checkups: List[Modbus_Checkup]
        modbus_port: str
        modbus_address: int
        modbus_baud: int
        modbus_tries: int
        mqtt_broker: str
        mqtt_port: int
        mqtt_timeout: int

    # overloaded MQTT funcitons from (mqtt.Client)
    def on_log(self, client, userdata, level, buff):
        if level != mqtt.MQTT_LOG_DEBUG:
            print(level)
            print(buff)
        if level == mqtt.MQTT_LOG_ERR:
            print ("ERROR")
            traceback.print_exc()
            self.running = False;

    def on_connect(self, client, userdata, flags, rc):
        print("Connected: " + str(rc))
        self.subscribe("reporter/checkup_req")

    def on_message(self, client, userdata, message):
        if message == "reporter/checkup_req":
            print("Checkup received.")
            self.checkup = True

    def bootup(self):
        self.notify_bootup()
        self.pings = 0

        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        self.running = True
        self.exiting = False
        self.checkup = False
        self.drive_run = False
        self.drive_reset = False

    def signal_handler(self, signum, frame):
        print("Caught a deadly signal!")
        self.running = False
        self.exiting = True

    def notify(self, path, params, retain=False):
        params['time'] = str(time.time())
        print(params)

        topic = self.data.name + '/' + path
        self.publish(topic, json.dumps(params), retain=retain)
        print("Published " + topic)

    def notify_bootup(self):
        boot_checks = {}

        print("Bootup:")

        for bc_name, bc_cmd in self.data.boot_check_list.items():
            boot_checks[bc_name] = subprocess.check_output(
                    bc_cmd, 
                    shell=True
            ).decode('utf-8')

        self.notify('bootup', boot_checks, retain=True)

    def renew(self):
        for try__ in range(3):
            try:
                checks = {}
                for try_ in range(self.data.modbus_tries):
                    try:
                        status_word = self.instr.read_register(5)
                        self.drive_ready = bool(status_word & (1<<5))
                        self.drive_tripped = bool(status_word & (1<<1))
                        self.drive_running = bool(status_word & (1<<0))
                        self.drive_error = status_word >> 8
                        break
                    except IOError:
                        if try_ == self.data.modbus_tries - 1:
                            self.running = False
                            self.exiting = True

                if self.checkup == True:
                    for check_name, check_register in self.data.modbus_checkups:
                        for try_ in range(self.data.modbus_tries):
                            try:
                                checks[check_name] = self.instr.read_register(check_register)
                                break
                            except IOError:
                                print("Failed to write to instrument")
                                traceback.print_exc()
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
                        for check_name, check_command in self.data.boot_check_list:
                            long_checkup_count += 1
                            if long_checkup_count > self.data.long_checkup_leng:
                                break
                            checks[check_name] = subprocess.check_output(
                                    check_command,
                                    shell=True
                            ).decode('utf-8')
        
                    self.notify('checkup', checks)
                    self.checkup = False
        
                # renew the on command
                for try_ in range(self.data.modbus_tries):
                    try:
                        control_word = bool(self.drive_run and self.drive_ready) & (bool(self.drive_reset) << 2)
                        self.drive_reset = False
                        self.instr.write_register(0, control_word)
                    except IOError:
                        print("Failed to write to instrument")
                        traceback.print_exc()
                        if try_ == self.data.modbus_tries - 1:
                            self.running = False
                            self.exiting = True
                break
            except:
                traceback.print_exc()
                if try__ == 2:
                    self.running = False
                    self.exiting = True
    

    def run(self):
        while True:
            self.running = True
            while self.running:
                try:
                    self.connect(self.data.mqtt_broker, self.data.mqtt_port,
                            self.data.mqtt_timeout)
                    self.instr = minimalmodbus.Instrument(self.data.modbus_port, 
                            self.data.modbus_address)
                    self.instr.serial.baudrate = self.data.modbus_baud
                    self.bootup()
                    timer = MultiTimer(interval=1, function = self.renew)
                    timer.start()

                    while self.running:
                        self.loop()

                    timer.stop()
                    self.disconnect()
                except:
                    traceback.print_exc()
                    timer.stop()

                    pass

                if self.exiting:
                    exit(0)
