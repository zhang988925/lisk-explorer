/*
 * LiskHQ/lisk-explorer
 * Copyright © 2018 Lisk Foundation
 *
 * See the LICENSE file at the top-level directory of this distribution
 * for licensing information.
 *
 * Unless otherwise agreed in a custom licensing agreement with the Lisk Foundation,
 * no part of this software, including this file, may be copied, modified,
 * propagated, or distributed except according to the terms contained in the
 * LICENSE file.
 *
 * Removal or modification of this copyright notice is prohibited.
 *
 */
const AbstractCandles = require('./abstract');
const util = require('util');
const _ = require('underscore');
const async = require('async');
const logger = require('../../utils/logger');
const request = require('request');
const moment = require('moment');

function BittrexCandles(...rest) {
	AbstractCandles.apply(this, rest);

	const self = this;

	this.name = 'bittrex';
	this.key = `${this.name}Candles`;
	this.url = 'https://api.binance.com/api/v1/klines?symbol=LSKBTC&interval=1m';
	this.start = '';
	this.last = null;

	this.response = {
		error: 'message',
		data: 'result',
	};

	this.candle = {
		id: 'date',
		date: 'date',
		open: 'open',
		close: 'close',
		high: 'high',
		low: 'low',
		liskVolume: 'liskVolume',
		btcVolume: 'btcVolume',
	};

	this.validationKey = 'date';

	this.retrieveTrades = function (start, end, cb) {
		let found = false;
		let results = [];

		logger.info(`Candles: Retrieving trades from ${this.name}...`);

		async.doUntil(
			(next) => {
				logger.info(`Candles: Start: ${start || 'N/A'}`);
				logger.info(this.url + (start ? `&startTime=${start}` : ''));
				request.get({
					url: this.url + (start ? `&startTime=${start}` : ''),
					json: true,
				}, (err, resp, body) => {
					if (err || resp.statusCode !== 200) {
						if (resp.statusCode === 400 && body.code === -1104) {
							found = true;
							return next();
						}
						return next(err || 'Response was unsuccessful');
					}

					let parsedBody;
					if (typeof parsedBody === 'string') {
						try {
							parsedBody = JSON.parse(body);
						} catch (jsonError) {
							return next(`Error while parsing JSON: ${jsonError.message}`);
						}
					} else {
						parsedBody = body;
					}

					// eslint-disable-next-line arrow-body-style
					const dataAsObject = body.map((o) => {
						return {
							date: o[0],
							open: o[1],
							high: o[2],
							low: o[3],
							close: o[4],
							liskVolume: o[5],
							btcVolume: 0,
							numberOfTrades: o[9],
						};
					});

					const message = body[this.response.error];
					if (message) {
						return next(message);
					}

					const data = this.rejectTrades(dataAsObject);
					if (!this.validData(data)) {
						logger.error('Candles:', 'Invalid data received');
						return next();
					}

					if (this.validTrades(results, data)) {
						logger.info(['Candles:', (start ? start.toString() : 'N/A'), 'to', (end ? end.toString() : 'N/A'), '=> Found', data.length.toString(), 'trades'].join(' '));
						results = this.acceptTrades(results, data);

						start = this.nextStart(data);

						found = false;
						return next();
					}
					found = true;
					return next();
				});
			},
			() => found,
			(err) => {
				if (err) {
					return cb(`Error retrieving trades: ${err}`);
				}
				logger.info(`Candles: ${results.length.toString()} trades in total retrieved`);
				return cb(null, results);
			});
	};

	this.nextStart = function (data) {
		const sortedData = _.sortBy(data, this.candle.date);
		return sortedData[sortedData.length - 1][this.candle.date];
	};

	this.parser = function (period) {
		const sortedPeriodList = _.sortBy(period, t => this.parseDate(t[this.candle.date]).unix());
		const earliestDateItem = sortedPeriodList[0];
		const latestDateItem = sortedPeriodList[sortedPeriodList.length - 1];

		return {
			timestamp: this.parseDate(earliestDateItem[this.candle.date]).unix(),
			date: this.parseDate(earliestDateItem[this.candle.date]).toDate(),
			high: _.max(period, t => parseFloat(t[this.candle.high]))[this.candle.high],
			low: _.min(period, t => parseFloat(t[this.candle.low]))[this.candle.low],
			open: earliestDateItem[this.candle.open],
			close: latestDateItem[this.candle.close],
			liskVolume: _.reduce(period, (memo, t) =>
				(memo + parseFloat(t[this.candle.liskVolume])), 0.0).toFixed(8),
			btcVolume: _.reduce(period, (memo, t) =>
				(memo + (parseFloat(t[this.candle.btcVolume]))), 0.0)
				.toFixed(8),
		};
	};

	this.acceptTrades = function (results, data) {
		return results.concat(data.reverse());
	};

	const _dropAndSave = function (trades, cb) {
		async.waterfall([
			function (waterCb) {
				return self.dropCandles(waterCb);
			},
			function (waterCb) {
				return self.saveCandles(trades, waterCb);
			},
		],
		(err, results) => {
			if (err) {
				return cb(err);
			}
			return cb(null, results);
		});
	};

	const _updateCandles = function (trades, cb) {
		logger.info(`Candles: Updating ${self.duration} candles for ${self.name}...`);

		async.waterfall([
			function (waterCb) {
				return self.groupTrades(trades, waterCb);
			},
			function (results, waterCb) {
				return self.sumTrades(results, waterCb);
			},
			function (results, waterCb) {
				return _dropAndSave(results, waterCb);
			},
		],
		(err, results) => {
			if (err) {
				return cb(err);
			}
			return cb(null, results);
		});
	};

	this.updateCandles = function (cb) {
		async.waterfall([
			function (waterCb) {
				return self.retrieveTrades(null, null, waterCb);
			},
			function (trades, waterCb) {
				async.eachSeries(self.durations, (duration, eachCb) => {
					self.duration = duration;
					return _updateCandles(trades, eachCb);
				}, (err) => {
					if (err) {
						return waterCb(err);
					}
					return waterCb(null);
				});
			},
		],
		(err) => {
			if (err) {
				return cb(err);
			}
			return cb(null);
		});
	};


	this.parseDate = function (date) {
		return moment.utc(date).startOf(self.duration);
	};
}

util.inherits(BittrexCandles, AbstractCandles);
module.exports = BittrexCandles;
