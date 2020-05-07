const webpack = require('webpack')

module.exports = {
	mode: 'production',
	target: 'node',
	entry: './lib/cli.js',
	module: {
		rules: [{
			test: /cli\.js$/,
			loader: require.resolve('shebang-loader')
		}]
	},
	output: {
		filename: 'dmut.js'
	},
	plugins: [
    new webpack.BannerPlugin({ banner: "#!/usr/bin/env node", raw: true }),
	],
	externals: ['pg-native']
}