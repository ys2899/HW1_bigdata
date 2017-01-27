import argparse
import logging
import json
import time
import schedule
import atexit

from kafka import KafkaProducer
from googlefinance import getQuotes
from kafka.errors import KafkaError

# - logging
logging.basicConfig()
logger = logging.getLogger('data-producer')

# - debug, nfo, warning, error
logger.setLevel(logging.DEBUG)


symbol = 'AAPL';
kafka_broker = '127.0.0.1:9092';
topic  = 'stock-analyzer';

def fetch_price(producer, symbol):
	logger.debug('start to fetch price for %s' % symbol);
	price = json.dumps(getQuotes(symbol))
	print price
	producer.send(topic = topic, value = price, timestamp_ms = time.time())
	logger.debug('sent stock price for  %s, price is %s' %(symbol, price))
	## logger.debug() is talking about the message that you want to convey.

def shut_down(producer, symbol):
	logger.debug('exiting program')
	producer.flush(10);
	producer.close('kafka producer closed, exiting')
	logger.debug('kafka producer closed, exiting')

if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('symbol', help = 'the stock symbol, such as AAPL');
	parser.add_argument('kafka_broker', help = 'the location of the kafkaproker')
	parser.add_argument('topic', help = 'the kafka topic write to')

	args = parser.parse_args()
	symbol = args.symbol
	topic = args.topic
	kafka_broker = args.kafka_broker

	logger.debug('stock symbol is %s' % symbol);

	# - initiate a kafka producer

	producer = KafkaProducer(
			bootstrap_servers = kafka_broker
		)

	schedule.every(1).second.do(fetch_price, producer, symbol)

	atexit.register(shut_down, producer);

	while True:
		schedule.run_pending()
		time.sleep(1)







