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
const moment = require('moment');
const _ = require('underscore');
const util = require('util');
const logger = require('../../utils/logger');
const request = require('request');
const async = require('async');

function BinanceCandles(...rest) {
	AbstractCandles.apply(this, rest);

	this.start = null;
	this.end = null; // Current unix timestamp (in sec)

	this.name = 'binance';
	this.key = `${this.name}Candles`;
	this.url = 'https://api.binance.com/api/v1/aggTrades?symbol=LSKBTC';

	this.response = {
		error: 'error',
		data: null,
	};

	this.candle = {
		id: 'a',
		date: 'T',
		price: 'p',
		amount: 'q',
	};

	this.duration = 'hour';
	this.durations = ['hour', 'day'];

	this.validationKey = 'a';

	this.retrieveTrades = function (start, end, cb) {
		let found = false;
		let results = [];

		logger.info(`Candles: Retrieving trades from ${this.name}...`);

		async.doUntil(
			(next) => {
				logger.info(`Candles: Start: ${start || 'N/A'}`);
				logger.info(this.url + (start ? `&fromId=${start}` : ''));
				request.get({
					url: this.url + (start ? `&fromId=${start}` : ''),
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

					const message = body[this.response.error];
					if (message) {
						return next(message);
					}

					const data = this.rejectTrades(parsedBody[this.response.data] || parsedBody);
					if (!this.validData(data)) {
						logger.error('Candles:', 'Invalid data received');
						return next();
					}

					if (this.validTrades(results, data)) {
						logger.info(['Candles:', (start ? start.toString() : 'N/A'), 'to', (end ? end.toString() : 'N/A'), '=> Found', data.length.toString(), 'trades'].join(' '));
						results = this.acceptTrades(results, data);

						start = this.nextStart(data);
						// end = this.nextEnd(data);
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

	this.nextEnd = function (data) {
		const sortedData = _.sortBy(data, this.candle.date);
		const span = parseInt(sortedData[sortedData.length - 1][this.candle.date], 10)
		- parseInt(sortedData[0][this.candle.date], 10);
		return parseInt(sortedData[sortedData.length - 1][this.candle.date], 10) + span;
	};

	this.nextStart = function (data) {
		const sortedData = _.sortBy(data, this.candle.id);
		return sortedData[sortedData.length - 1][this.candle.id];
	};

	this.rejectTrades = function (data) {
		if (this.last) {
			return _.reject(data, trade =>
				(!trade ? true : trade[this.candle.id] <= this.last.lastTrade));
		}
		return data;
	};

	this.validData = function (data) {
		if (_.size(data) > 0) {
			return _.first(data)[this.validationKey];
		}
		return true;
	};

	this.validTrades = function (results, data) {
		const matching = 'MIN_MAX';
		const any = _.size(data) > 0;

		/* eslint-disable */
		function eqSet(as, bs) {
			if (as.size !== bs.size) return false;
			for (var a of as) if (!bs.has(a)) return false;
			return true;
		}

		/* eslint-disable */
		if (any && _.size(results) > 0) {
			switch (matching) {
				case 'FULL':
					const set1 = new Set(results.map(o => o[this.candle.id]));
					const set2 = new Set(data.map(o => o[this.candle.id]));
					return !eqSet(set1, set2);
				case 'MIN_MAX':
					const dataMax = _.max(data, o => Number(o[this.candle.id]))[this.candle.id];
					const dataMin = _.min(data, o => Number(o[this.candle.id]))[this.candle.id];
					const resultsMax = _.max(results, o => Number(o[this.candle.id]))[this.candle.id];
					const resultsMin = _.min(results, o => Number(o[this.candle.id]))[this.candle.id];
					return !(dataMin === resultsMin || dataMax === resultsMax);
				case 'FIRST_LAST':
				default:
					const firstLast = _.first(results)[this.candle.id] !== _.last(data)[this.candle.id];
					const lastFirst = _.last(results)[this.candle.id] !== _.first(data)[this.candle.id];
					return (firstLast && lastFirst);				
			}
		}
		return any;
	};

	this.parser = function (period) {
		return {
			timestamp: this.parseDate(period[0][this.candle.date]).unix() / 1000,
			date: this.parseDate(new Date(period[0][this.candle.date])).toDate(),
			high: _.max(period, t => parseFloat(t[this.candle.price]))[this.candle.price],
			low: _.min(period, t => parseFloat(t[this.candle.price]))[this.candle.price],
			open: _.first(period)[this.candle.price],
			close: _.last(period)[this.candle.price],
			liskVolume: _.reduce(period, (memo, t) =>
				(memo + parseFloat(t[this.candle.amount])), 0.0).toFixed(8),
			btcVolume: _.reduce(period, (memo, t) =>
				(memo + (parseFloat(t[this.candle.amount]) * parseFloat(t[this.candle.price]))), 0.0)
				.toFixed(8),
			firstTrade: _.first(period)[this.candle.id],
			lastTrade: _.last(period)[this.candle.id],
			nextEnd: this.nextEnd(period),
			numTrades: 0,
		};
	};

	this.nextEnd = function (data) {
		return moment(_.first(data).date).subtract(1, 's').unix();
	};

	this.acceptTrades = function (results, data) {
		return results.concat(data.reverse());
	};

	this.parseDate = function (date) {
		return moment.utc(date).startOf(this.duration);
	};
}

util.inherits(BinanceCandles, AbstractCandles);
module.exports = BinanceCandles;
