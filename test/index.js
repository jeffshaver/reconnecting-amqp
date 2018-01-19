const { ReconnectingAMQP } = require('../index')

const amqp = new ReconnectingAMQP('amqp://guest:guest@192.168.99.100:32098', {})

amqp.connect()
