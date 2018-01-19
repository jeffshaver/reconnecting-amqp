const { URL } = require('url')
const amqp = require('amqplib')
const debug = require('debug')
const info = debug('reconnecting-amqp:info')
const warn = debug('reconnecting-amqp:warn')

const _onClose = Symbol('_onClose')
const _reconsume = Symbol('_reconsume')

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
      this.channel = await this.connection.createChannel()
      info('Successfully created channel')

      this.connection.on('close', this[_onClose])
    } catch (e) {
      warn(e)
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

  async [_onClose]() {
    warn('AMQP connection closed. Reconnecting...')
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
