import paho.mqtt.client as mqtt
import traceback, os
from daemon import Daemon
from dataclasses import dataclass
from dataclasses_json import dataclass_json
from typing import *
from multitimer import Multitimer

class HFCDaemon(Daemon):
    def run(self):
        h_fanContoller = HFC()
        my_path = os.path.dirname(os.path.abspath(__file__))
        config = open(my_path + "/hfc_config.json", "r")
        h_fanController.data = HFC.data.from_json(config.read())
        config.close()

        h_fanController.run()


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
        long_checkup_length: int
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
        print("Checkup received.")
        self.checkup = True

    def bootup(self):
        self.notify_bootup()
        self.pings = 0

        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        self.running = True
        self.exiting = False

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
        checks = {}
        if self.checkup = True
            for checkup in checkups:
                for try_ in range(0,self.data.modbus_tries)
                    try:
                        checks[checkup.name] = instr.read_register(checkup.num)
                        break
                    except IOError:
                        print("Failed to write to instrument")
                        traceback.print_exc()


            self.pings += 1
            if(self.pings % self.data.long_checkup_freq == 0):
                self.pings = 0
                long_checkup_count = 0
                for check_name in self.data.boot_check_list:
                    long_checkup_count += 1
                    if long_checkup_count > self.data.long_checkup_leng:
                        break
                    checks[check_name] = subprocess.check_output(
                            self.data.boot_check_list[check_name],
                            shell=True
                    ).decode('utf-8')

            self.notify('checkup', checks)
            self.checkup = False

        # renew the on command
        for try_ in range(0,self.data.modbus_tries)
            try:
                instr.write_register(0, 1)
            except IOError:
                print("Failed to write to instrument")
                traceback.print_exc()


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
                    timer = MultiTimer(interval=1, funciton = self.renew)
                    timer.start()

                    while self.running:
                        self.loop()

                    timer.stop()
                except:
                    timer.stop()
                    traceback.print_exc()

                    pass

                if self.exiting:
                    exit(0)
