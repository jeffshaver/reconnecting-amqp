const { URL } = require('url')
const amqp = require('amqplib')
const debug = require('debug')
const info = debug('reconnecting-amqp:info')
const warn = debug('reconnecting-amqp:warn')

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

      this.connection.on('close', () => {
        warn('AMQP connection closed. Reconnecting...')
        this.connect()
      })
    } catch (e) {
      warn(e)
    }
  }

  async consume(queue, onConsume) {
    try {
      await this.channel.assertQueue(queue)

      this.channel.consume(queue, message => {
        if (message === null) {
          warn(`Message from ${queue} is null`)

          return
        }

        info(message.content)

        onConsume(message)

        this.channel.ack(message)
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
}

module.exports = { ReconnectingAMQP }
