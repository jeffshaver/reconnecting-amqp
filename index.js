const { URL } = require('url')
const amqp = require('amqplib')
const debug = require('debug')
const info = debug('reconnecting-amqp:info')
const warn = debug('reconnecting-amqp:warn')

const _delay = Symbol('_delay')
const _updateReconnectDelay = Symbol('_updateReconnectDelay')
const _onClose = Symbol('_onClose')
const _reconsume = Symbol('_reconsume')
const _reconnect = Symbol('_reconnect')

class ReconnectingAMQP {
  constructor(endpoint, options) {
    const url = new URL(endpoint)

    this.connection = null
    this.channel = null
    this.endpoint = endpoint
    this.options = options
    this.protocol = url.protocol
    this.hostname = url.hostname
    this.port = url.port
    this.minReconnectDelay = 1500
    this.maxReconnectDelay = 10000
    this.reconnectCount = 0
    this.reconnectDelay = 0
    this.reconnectDelayGrowthFactor = 1.3
    /*
    consumer = {
      queue: String,
      onConsume: Function,
      autoAck: Boolean
    }
    */
    this.consumers = []

    this[_onClose] = this[_onClose].bind(this)
  }

  async connect() {
    try {
      this.connection = await amqp.connect(this.endpoint, this.options)
      info(
        'Successfully connected to ' +
          `${this.protocol}//${this.hostname}:${this.port}`
      )
      this.reconnectCount = 0
      this.reconnectDelay = 0
      this.channel = await this.connection.createChannel()
      info('Successfully created channel')

      this.connection.on('close', this[_onClose])
    } catch (e) {
      warn(e)
      this[_reconnect]()
    }
  }

  async consume(queue, onConsume, autoAck = false) {
    this.consumers.push({
      queue,
      onConsume,
      autoAck
    })

    try {
      await this.channel.assertQueue(queue)

      this.channel.consume(queue, message => {
        if (message === null) {
          warn(`Message from ${queue} is null`)

          return
        }

        onConsume(message)

        if (autoAck) {
          this.channel.ack(message)
        }
      })
    } catch (e) {
      warn(e)
    }
  }

  async sendToQueue(queue, message) {
    try {
      await this.channel.assertQueue(queue)
      this.channel.sendToQueue(queue, Buffer.from(message))
    } catch (e) {
      warn(e)
      channel.close()
      connection.close()
    }
  }

  [_delay](ms) {
    return new Promise(resolve => setTimeout(resolve, ms))
  }

  [_updateReconnectDelay]() {
    if (this.reconnectDelay === 0) {
      this.reconnectDelay =
        this.minReconnectDelay + Math.random() * this.minReconnectDelay
    }

    const reconnectDelay = this.reconnectDelay * this.reconnectDelayGrowthFactor

    this.reconnectDelay =
      reconnectDelay > this.maxReconnectDelay
        ? this.maxReconnectDelay
        : reconnectDelay
  }

  async [_onClose]() {
    warn('AMQP connection closed.')
    await this[_reconnect]()
    this[_reconsume]()
  }

  async [_reconnect]() {
    this.reconnectCount = this.reconnectCount + 1
    this[_updateReconnectDelay]()
    info(`Will attempt reconnect in ${this.reconnectDelay / 1000}s`)
    await this[_delay](this.reconnectDelay)
    info(`Reconnecting to ${this.protocol}//${this.hostname}:${this.port}...`)
    await this.connect()
    this[_reconsume]()
  }

  async [_reconsume]() {
    this.consumers.forEach(({ queue, onConsume, autoAck }) => {
      this.consume(queue, onConsume, autoAck)
    })
  }
}

module.exports = { ReconnectingAMQP }
