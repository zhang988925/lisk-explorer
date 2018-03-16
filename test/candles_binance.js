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
const config = require('../config');
const client = require('../redis')(config);
const candles = require('../lib/candles');
const async = require('async');

/* eslint-disable no-console */
/* eslint-disable wrap-iife */
(function () {
	async.series([
		(callback) => {
			const binance = new candles.binanceHourDay(client, config.marketWatcher.candles.binance);

			return binance.buildCandles((err, res) => {
				if (err) {
					return callback(err);
				}
				return callback(null, res);
			});
		},
		(callback) => {
			const binance = new candles.binanceMinute(client, config.marketWatcher.candles.binance);

			return binance.buildCandles((err, res) => {
				if (err) {
					return callback(err);
				}
				return callback(null, res);
			});
		},
		(callback) => {
			const binance = new candles.binanceCandles(client, config.marketWatcher.candles.binance);

			return binance.buildCandles((err, res) => {
				if (err) {
					return callback(err);
				}
				return callback(null, res);
			});
		},
	],
	(err) => {
		if (err) {
			console.log(err);
		}
	});
})();
