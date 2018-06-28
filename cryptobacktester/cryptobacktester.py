import datetime
import json
import logging
import os
from pprint import pprint
import sys
import time

from indicatorcalc_redux import IndicatorCalc
import numpy as np
from pymarketcap import Pymarketcap
import requests

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

config_path = '../../TeslaBot/config/config.ini'


class CryptoBacktester:
    def __init__(self, allowed_exchanges):
        self.allowed_exchanges = allowed_exchanges

        self.cmc = Pymarketcap()

        self.indicator_calc = IndicatorCalc()


    def filter_markets(self):
        ranks_filtered = {'gainers': {'1h': [], '24h': [], '7d': []},
                          'losers': {'1h': [], '24h': [], '7d': []}}

        failed_products = {'gainers': {'1h': [], '24h': [], '7d': []},
                           'losers': {'1h': [], '24h': [], '7d': []}}

        time_bins = ['1h', '24h', '7d']

        ranks = self.cmc.ranks()

        gainers = ranks['gainers']
        losers = ranks['losers']

        for bin in time_bins:
            logger.debug('bin: ' + str(bin))

            for mkt in gainers[bin]:
                logger.debug('[gainers] mkt: ' + str(mkt))

                try:
                    markets = self.cmc.markets(mkt['website_slug'])

                    for exch in markets['markets']:
                        if exch['source'].lower() in self.allowed_exchanges:
                            ranks_filtered['gainers'][bin].append((mkt, exch))

                except Exception as e:
                    logger.exception(e)

                    failed_products['gainers'][bin].append(mkt)

            for mkt in losers[bin]:
                logger.debug('[losers] mkt: ' + str(mkt))

                try:
                    markets = self.cmc.markets(mkt['website_slug'])

                    for exch in markets['markets']:
                        if exch['source'].lower() in self.allowed_exchanges:
                            ranks_filtered['losers'][bin].append((mkt, exch))

                except Exception as e:
                    logger.exception(e)

                    failed_products['losers'][bin].append(mkt)

        return ranks_filtered, failed_products


    def get_best_pairs(self, ranked_products):
        best_pairs = {'success': True, 'result': {}}

        #conversion_currencies = ['BTC', 'ETH', 'USD']

        try:
            for rank_type in ranked_products:
                logger.debug('rank_type: ' + rank_type)

                best_pairs['result'][rank_type] = {}

                for time_bin in ranked_products[rank_type]:
                    logger.debug('time_bin: ' + time_bin)

                    best_pairs['result'][rank_type][time_bin] = {}

                    for product in ranked_products[rank_type][time_bin]:
                        logger.debug('product: ' + str(product))

                        if product[0]['symbol'] not in best_pairs['result'][rank_type][time_bin]:
                            best_pairs['result'][rank_type][time_bin][product[0]['symbol']] = {}

                        if product[1]['pair'].split('/')[1] not in best_pairs['result'][rank_type][time_bin][product[0]['symbol']]:
                            best_pairs['result'][rank_type][time_bin][product[0]['symbol']][product[1]['pair'].split('/')[1]] = self.cmc.ticker(currency=product[0]['website_slug'],
                                                                                                                                                convert=product[1]['pair'].split('/')[1])['data']['quotes'][product[1]['pair'].split('/')[1]]

                            time.sleep(2)

        except Exception as e:
            logger.exception('Exception raised in get_best_pairs().')
            logger.exception(e)

            best_pairs['success'] = False

        finally:
            return best_pairs


    def get_candles(self, exchange, market, interval=0):
        candles = {'success': True, 'result': {}}

        #try:
        logger.debug('exchange: ' + exchange)
        logger.debug('market: ' + market)
        logger.debug('interval: ' + interval)

        valid_intervals = [60, 180, 300, 900, 1800, 3600, 7200,
                           14400, 21600, 43200, 86400, 259200, 604800]

        endpoint = '/markets/' + exchange.lower() + '/' + market.lower() + '/ohlc'

        url = 'https://api.cryptowat.ch' + endpoint

        url_params = {}

        if interval == 0:
            pass

        else:
            candle_url_param = str(int(round(float(interval), 0)))

            url_params['periods'] = candle_url_param

            if interval not in valid_intervals:
                logger.error('Invalid interval passed to get_candles(). Exiting.')

                sys.exit(1)

        try:
            r = requests.get(url, params=url_params)

            time.sleep(request_delay)

            results = r.json()

            if 'result' not in results or 'allowance' not in results:
                logger.debug('[get_candles()] Failed to acquire valid candles.')

                candles['success'] = False

                if 'error' in results:
                    logger.error('Error while calling Cryptowat.ch API.')
                    logger.error(results['error'])

                    if results['error'] == 'Out of allowance':
                        allowance_remaining = 0

            else:
                allowance_remaining = results['allowance']['remaining']
                allowance_cost = results['allowance']['cost']

                #allowance_avg_cost = average_api_cost(allowance_cost)

                if candles['success'] == True:
                    for time_bin in results['result']:
                        data = results['result'][time_bin]

                        np_historical = np.array(data, dtype='f8')

                        candles[time_bin] = {}

                        candles[time_bin]['close_time'] = np_historical[:, 0]
                        candles[time_bin]['open'] = np_historical[:, 1]
                        candles[time_bin]['high'] = np_historical[:, 2]
                        candles[time_bin]['low'] = np_historical[:, 3]
                        candles[time_bin]['close'] = np_historical[:, 4]
                        candles[time_bin]['volume'] = np_historical[:, 5]

        except requests.exceptions.RequestException as e:
            logger.exception('RequestException while retrieving candles.')
            logger.exception(e)

            #candle_data['RequestException'] = True
            candles['success'] = False

        except requests.exceptions.ConnectionError as e:
            logger.error('ConnectionError while retrieving candles.')
            logger.error(e)

            #candle_data['Error'] = True
            candles['success'] = False

        except json.JSONDecodeError as e:
            logger.error('JSONDecodeError while retrieving candles.')
            logger.error(e)

            #candle_data['Error'] = True
            candles['success'] = False

        except Exception as e:
            logger.exception('Uncaught exception while retrieving candles.')
            logger.exception(e)

            #candle_data['Exception'] = True
            candles['success'] = False

        finally:
            return candles


if __name__ == '__main__':
    try:
        allowed_exchanges = ['binance', 'bittrex', 'gdax', 'poloniex']

        crypto_backtester = CryptoBacktester(allowed_exchanges)

        """
        ranks_filtered, failed_products = crypto_backtester.filter_markets()

        if not os.path.exists('json/'):
            logger.info('Creating json directory.')

            os.mkdir('json/')
        """

        dt_current = datetime.datetime.now().strftime('%m%d%Y-%H%M%S')

        """
        logger.info('Dumping results to json file.')

        ranks_json_file = 'json/' + dt_current + '_ranks.json'

        with open(ranks_json_file, 'w', encoding='utf-8') as file:
            json.dump(ranks_filtered, file, indent=4, sort_keys=True, ensure_ascii=False)

        logger.info('Gathering candles for ranked products from selected exchanges.')

        for rank_type in ranks_filtered:
            for time_bin in ranks_filtered[rank_type]:
                pass
        """

        test_json_file = 'test.json'

        with open(test_json_file, 'r', encoding='utf-8') as file:
            data = json.load(file)

        best_pairs = crypto_backtester.get_best_pairs(ranked_products=data)

        #print('BEST PAIRS:')
        #pprint(best_pairs)

        logger.info('Dumping best pairs data to json file.')

        pairs_json_file = 'json/' + dt_current + '_pairs.json'

        with open(pairs_json_file, 'w', encoding='utf-8') as file:
            json.dump(best_pairs, file, indent=4, sort_keys=True, ensure_ascii=False)

        logger.info('Done.')

    except Exception as e:
        logger.exception(e)

    except KeyboardInterrupt:
        logger.info('Exit signal received.')

    finally:
        logger.info('Exiting.')
