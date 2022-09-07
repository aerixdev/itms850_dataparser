'''
스마트콤보(제품명 : ITMS-850) 센서 데이터 해석 예제 프로그램

작성자 : 이상훈 (에어릭스 환경시스템사업부 기술연구소&개발팀, sanghoon.lee@aerix.co.kr)
작성일 : 2022-09-06
'''

import logging
import paho.mqtt.client as mqtt
import json

'''
ITMS-850 센서 데이터를 해석하는 함수
'''
def itms_data_parser(payload):
    cmd = payload[0]
    if(cmd!=1):
        logger.error("Error : invalid command")
    else:
        logger.info("Command is valid")

    header = payload[1]

    pres_sign = 0x01 & (header >> 5)
    temp_sign = 0x01 & (header >> 4)

    di4 = 0x01 & (header >> 3)
    di3 = 0x01 & (header >> 2)
    di2 = 0x01 & (header >> 1)
    di1 = 0x01 & header

    logger.info("PRESSURE SIGN : {} / TEMPERATURE SIGN : {} / DI4 :{} / DI3 :{} / DI2 :{} / DI1 :{}".format(pres_sign,temp_sign,di4,di3,di2,di1))

    ai1 = payload[2]*0x100+payload[3]
    ai2 = payload[4]*0x100+payload[5] 

    logger.info("AI1 : {} / AI2 : {}".format(ai1,ai2)) 

    diff_pres = payload[6]*0x100+payload[7]
    pres = payload[8]*0x100+payload[9]
    if(pres_sign==1):
        pres = 0 - pres # 음수로 변환
    temp = payload[10]*0x100+payload[11]
    temp = temp/100
    volt = payload[12]*0x100+payload[13]
    current = payload[14]*0x1000000+payload[15]*0x10000+payload[16]*0x100+payload[17]
    power = payload[18]*0x1000000+payload[19]*0x10000+payload[20]*0x100+payload[21]
    active_power = payload[22]*0x1000000+payload[23]*0x10000+payload[24]*0x100+payload[25]

    logger.info("DIFFERENTIAL PRESSURE: {} / PRESSURE : {} / TEMPERATURE :{} / VOLTAGE :{} / CURRENT :{} / POWER :{} / ACTIVE_POWER :{}".format(diff_pres,pres,temp,volt,current,power,active_power))
'''
MQTT 메시지가 수신될 때 호출되는 콜백 함수
'''
def on_message(client,userdata,msg):
    logger.info("Received Message : {}".format(msg.payload))

    json_data = json.loads(msg.payload)
    # MQTT 메시지에 'mac' 키가 존재하면 스마트콤보의 센서 데이터가 아닌 BLE 무선센서의 데이터이기 떄문에
    # 이 예제 프로그램에서는 처리하지 않도록 구현했습니다. 
    if 'mac' in json_data:
        return

    payload = json_data['payload']
    
    itms_data_parser(payload)

'''
MQTT 브로커에 연결될 떄 호출되는 콜백 함수
'''
def on_connect(client,userdata,flags,rc):
    global logger
    
    if(rc==0):
        logger.info("MQTT broker is connected.")
    else:
        logger.info("Connection error is occured.")

'''
MQTT 브로커에 연결이 해제될 떄 호출되는 콜백 함수
'''
def on_disconnect(client,userdata,flags,rc):
    logger.info("MQTT broker is disconnected.")

'''
logger 객체를 초기화하는 함수
'''
def logger_init(logger_name):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    streamHandler = logging.StreamHandler()
    streamHandler.setFormatter(formatter)
    logger.addHandler(streamHandler)

    return logger

'''
Main 함수
'''
def main():
    global logger
    logger = logger_init("ITMS-850")

    logger.info("Application is started.........................")

    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message

    # MQTT 접속 정보는 config.json 파일에서 읽어옵니다.
    with open("./config.json",'r') as f:
        config_data = json.load(f)

        server = config_data['server']
        port = config_data['port']
        topic = config_data['topic']

    logger.info("server : {}".format(server))
    logger.info("port : {}".format(port))
    logger.info("topic : {}".format(topic))

    # MQTT 브로커에 접속 및 메시지 구독
    client.connect(server,port)
    client.subscribe(topic)
    client.loop_forever()

if __name__=="__main__":
    main()