# freqtrade-stuff
Use the examples / information here at your own risk.

If you don't care about how to use that code then you dont need to look at it.

Inside the .7z file you see files sorted by exchange and min_price.

What I mostly use is the monthly file and then when a backtest should be started from a certain month the previous months' pairlist is taken and used as static list. Just copy out what block you need.
With this the the behavior is closer to live since volumes of pairs change over time.
