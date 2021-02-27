import zip from 'lodash.zipobject'

import httpMethods from 'http-client'
import openWebSocket from 'open-websocket'

const endpoints = {
  base: 'wss://stream.binance.us:9443/ws',
}

const depthTransform = m => ({
  eventType: m.e,
  eventTime: m.E,
  symbol: m.s,
  firstUpdateId: m.U,
  finalUpdateId: m.u,
  bidDepth: m.b.map(b => zip(['price', 'quantity'], b)),
  askDepth: m.a.map(a => zip(['price', 'quantity'], a)),
})

const depth = (payload, cb, transform = true, variator) => {
  const cache = (Array.isArray(payload) ? payload : [payload]).map(symbol => {
    const [symbolName, updateSpeed] = symbol.toLowerCase().split('@');
    const w = openWebSocket(
      `${endpoints.base}/${symbolName}@depth${updateSpeed ? `@${updateSpeed}` : ''}`,
    )
    w.onmessage = msg => {
      const obj = JSON.parse(msg.data)

      cb(
        transform
          ? depthTransform(obj)
          : obj,
      )
    }

    return w
  })

  return options =>
    cache.forEach(w => w.close(1000, 'Close handle was called', { keepClosed: true, ...options }))
}

const partialDepthTransform = (symbol, level, m) => ({
  symbol,
  level,
  lastUpdateId: m.lastUpdateId,
  bids: m.bids.map(b => zip(['price', 'quantity'], b)),
  asks: m.asks.map(a => zip(['price', 'quantity'], a)),
})


const partialDepth = (payload, cb, transform = true, variator) => {
  const cache = (Array.isArray(payload) ? payload : [payload]).map(({ symbol, level }) => {
    const [symbolName, updateSpeed] = symbol.toLowerCase().split('@');
    const w = openWebSocket(
      `${endpoints.base}/${symbolName}@depth${level}${updateSpeed ? `@${updateSpeed}` : ''}`,
    )
    w.onmessage = msg => {
      const obj = JSON.parse(msg.data)

      cb(
        transform
          ? partialDepthTransform(symbol, level, obj)
          : obj,
      )
    }

    return w
  })

  return options =>
    cache.forEach(w => w.close(1000, 'Close handle was called', { keepClosed: true, ...options }))
}

const candles = (payload, interval, cb, transform = true, variator) => {
  if (!interval || !cb) {
    throw new Error('Please pass a symbol, interval and callback.')
  }

  const cache = (Array.isArray(payload) ? payload : [payload]).map(symbol => {
    const w = openWebSocket(
      `${endpoints.base}/${symbol.toLowerCase()}@kline_${interval}`,
    )
    w.onmessage = msg => {
      const obj = JSON.parse(msg.data)
      const { e: eventType, E: eventTime, s: symbol, k: tick } = obj
      const {
        t: startTime,
        T: closeTime,
        f: firstTradeId,
        L: lastTradeId,
        o: open,
        h: high,
        l: low,
        c: close,
        v: volume,
        n: trades,
        i: interval,
        x: isFinal,
        q: quoteVolume,
        V: buyVolume,
        Q: quoteBuyVolume,
      } = tick

      cb(
        transform
          ? {
              eventType,
              eventTime,
              symbol,
              startTime,
              closeTime,
              firstTradeId,
              lastTradeId,
              open,
              high,
              low,
              close,
              volume,
              trades,
              interval,
              isFinal,
              quoteVolume,
              buyVolume,
              quoteBuyVolume,
            }
          : obj,
      )
    }

    return w
  })

  return options =>
    cache.forEach(w => w.close(1000, 'Close handle was called', { keepClosed: true, ...options }))
}

const tickerTransform = m => ({
  eventType: m.e,
  eventTime: m.E,
  symbol: m.s,
  priceChange: m.p,
  priceChangePercent: m.P,
  weightedAvg: m.w,
  prevDayClose: m.x,
  curDayClose: m.c,
  closeTradeQuantity: m.Q,
  bestBid: m.b,
  bestBidQnt: m.B,
  bestAsk: m.a,
  bestAskQnt: m.A,
  open: m.o,
  high: m.h,
  low: m.l,
  volume: m.v,
  volumeQuote: m.q,
  openTime: m.O,
  closeTime: m.C,
  firstTradeId: m.F,
  lastTradeId: m.L,
  totalTrades: m.n,
})

const ticker = (payload, cb, transform = true, variator) => {
  const cache = (Array.isArray(payload) ? payload : [payload]).map(symbol => {
    const w = openWebSocket(
      `${endpoints.base}/${symbol.toLowerCase()}@ticker`,
    )

    w.onmessage = msg => {
      const obj = JSON.parse(msg.data)
      cb(
        transform
          ? tickerTransform(obj)
          : obj,
      )
    }

    return w
  })

  return options =>
    cache.forEach(w => w.close(1000, 'Close handle was called', { keepClosed: true, ...options }))
}

const allTickers = (cb, transform = true, variator) => {
  const w = new openWebSocket(
    `${endpoints.base}/!ticker@arr`,
  )

  w.onmessage = msg => {
    const arr = JSON.parse(msg.data)
    cb(
      transform
        ? arr.map(m => tickerTransform(m))
        : arr,
    )
  }

  return options => w.close(1000, 'Close handle was called', { keepClosed: true, ...options })
}

const aggTradesTransform = m => ({
  eventType: m.e,
  eventTime: m.E,
  timestamp: m.T,
  symbol: m.s,
  price: m.p,
  quantity: m.q,
  isBuyerMaker: m.m,
  wasBestPrice: m.M,
  aggId: m.a,
  firstId: m.f,
  lastId: m.l,
})

const aggTrades = (payload, cb, transform = true, variator) => {
  const cache = (Array.isArray(payload) ? payload : [payload]).map(symbol => {
    const w = openWebSocket(
      `${endpoints.base}/${symbol.toLowerCase()}@aggTrade`,
    )
    w.onmessage = msg => {
      const obj = JSON.parse(msg.data)

      cb(
        transform
          ? aggTradesTransform(obj)
          : obj,
      )
    }

    return w
  })

  return options =>
    cache.forEach(w => w.close(1000, 'Close handle was called', { keepClosed: true, ...options }))
}

const tradesTransform = m => ({
  eventType: m.e,
  eventTime: m.E,
  tradeTime: m.T,
  symbol: m.s,
  price: m.p,
  quantity: m.q,
  isBuyerMaker: m.m,
  maker: m.M,
  tradeId: m.t,
  buyerOrderId: m.b,
  sellerOrderId: m.a,
})

const trades = (payload, cb, transform = true) => {
  const cache = (Array.isArray(payload) ? payload : [payload]).map(symbol => {
    const w = openWebSocket(`${endpoints.base}/${symbol.toLowerCase()}@trade`)
    w.onmessage = msg => {
      const obj = JSON.parse(msg.data)

      cb(transform ? tradesTransform(obj) : obj)
    }

    return w
  })

  return options =>
    cache.forEach(w => w.close(1000, 'Close handle was called', { keepClosed: true, ...options }))
}

const userTransforms = {
  // https://github.com/binance-exchange/binance-official-api-docs/blob/master/user-data-stream.md#balance-update
  balanceUpdate: m => ({
    asset: m.a,
    balanceDelta: m.d,
    clearTime: m.T,
    eventTime: m.E,
    eventType: 'balanceUpdate',
  }),
  // https://github.com/binance-exchange/binance-official-api-docs/blob/master/user-data-stream.md#account-update
  outboundAccountInfo: m => ({
    eventType: 'account',
    eventTime: m.E,
    makerCommissionRate: m.m,
    takerCommissionRate: m.t,
    buyerCommissionRate: m.b,
    sellerCommissionRate: m.s,
    canTrade: m.T,
    canWithdraw: m.W,
    canDeposit: m.D,
    lastAccountUpdate: m.u,
    balances: m.B.reduce((out, cur) => {
      out[cur.a] = { available: cur.f, locked: cur.l }
      return out
    }, {}),
  }),
  // https://github.com/binance-exchange/binance-official-api-docs/blob/master/user-data-stream.md#account-update
  outboundAccountPosition: m => ({
    balances: m.B.map(({ a, f, l }) => ({ asset: a, free: f, locked: l })),
    eventTime: m.E,
    eventType: 'outboundAccountPosition',
    lastAccountUpdate: m.u,
  }),
  // https://github.com/binance-exchange/binance-official-api-docs/blob/master/user-data-stream.md#order-update
  executionReport: m => ({
    eventType: 'executionReport',
    eventTime: m.E,
    symbol: m.s,
    newClientOrderId: m.c,
    originalClientOrderId: m.C,
    side: m.S,
    orderType: m.o,
    timeInForce: m.f,
    quantity: m.q,
    price: m.p,
    executionType: m.x,
    stopPrice: m.P,
    icebergQuantity: m.F,
    orderStatus: m.X,
    orderRejectReason: m.r,
    orderId: m.i,
    orderTime: m.T,
    lastTradeQuantity: m.l,
    totalTradeQuantity: m.z,
    priceLastTrade: m.L,
    commission: m.n,
    commissionAsset: m.N,
    tradeId: m.t,
    isOrderWorking: m.w,
    isBuyerMaker: m.m,
    creationTime: m.O,
    totalQuoteTradeQuantity: m.Z,
    orderListId: m.g,
    quoteOrderQuantity: m.Q,
    lastQuoteTransacted: m.Y,
  }),
}

export const userEventHandler = (cb, transform = true) => msg => {
  const { e: type, ...rest } = JSON.parse(msg.data)

  cb(
    transform && userTransforms[type]
      ? userTransforms[type](rest)
      : { type, ...rest },
  )
}

const STREAM_METHODS = ['get', 'keep', 'close']

const capitalize = (str, check) => (check ? `${str[0].toUpperCase()}${str.slice(1)}` : str)

const getStreamMethods = (opts, variator = '') => {
  const methods = httpMethods(opts)

  return STREAM_METHODS.reduce(
    (acc, key) => [...acc, methods[`${variator}${capitalize(`${key}DataStream`, !!variator)}`]],
    [],
  )
}

export const keepStreamAlive = (method, listenKey) => method({ listenKey })

const user = (opts, variator) => (cb, transform) => {
  const [getDataStream, keepDataStream, closeDataStream] = getStreamMethods(opts, variator)

  let currentListenKey = null
  let int = null
  let w = null

  const keepAlive = isReconnecting => {
    if (currentListenKey) {
      keepStreamAlive(keepDataStream, currentListenKey).catch(() => {
        closeStream({}, true)

        if (isReconnecting) {
          setTimeout(() => makeStream(true), 30e3)
        } else {
          makeStream(true)
        }
      })
    }
  }

  const closeStream = (options, catchErrors) => {
    if (currentListenKey) {
      clearInterval(int)

      const p = closeDataStream({ listenKey: currentListenKey })

      if (catchErrors) {
        p.catch(f => f)
      }

      w.close(1000, 'Close handle was called', { keepClosed: true, ...options })
      currentListenKey = null
    }
  }

  const makeStream = isReconnecting => {
    return getDataStream()
      .then(({ listenKey }) => {
        w = openWebSocket(
          `${endpoints.base}/${listenKey}`,
        )
        w.onmessage = msg => userEventHandler(cb, transform)(msg)

        currentListenKey = listenKey

        int = setInterval(() => keepAlive(false), 50e3)

        keepAlive(true)

        return options => closeStream(options)
      })
      .catch(err => {
        if (isReconnecting) {
          setTimeout(() => makeStream(true), 30e3)
        } else {
          throw err
        }
      })
  }

  return makeStream(false)
}

export default opts => {
  if (opts && opts.wsBase) {
    endpoints.base = opts.wsBase
  }

  return {
    depth,
    partialDepth,
    candles,
    trades,
    aggTrades,
    ticker,
    allTickers,
    user: user(opts),

    marginUser: user(opts, 'margin'),
  }
}
