const express = require('express');
const amqp = require('amqplib');
const path = require('path');

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

let channel;

// Função para conectar ao RabbitMQ no Docker
async function conectarRabbitMQ() {
    try {
        const connection = await amqp.connect('amqp://localhost:5672');
        channel = await connection.createChannel();
        
        // Criamos uma exchange do tipo 'fanout' chamada 'pedidos_exchange'
        await channel.assertExchange('pedidos_exchange', 'fanout', { durable: true });
        
        console.log('📦 [PRODUTOR] Conectado ao RabbitMQ com sucesso!');
    } catch (error) {
        console.error('❌ Erro ao conectar ao RabbitMQ. Tentando novamente em 5s...', error.message);
        setTimeout(conectarRabbitMQ, 5000);
    }
}
conectarRabbitMQ();

// Rota HTTP para receber o pedido do cliente e postar no RabbitMQ
app.post('/api/pedidos', async (req, res) => {
    if (!channel) {
        return res.status(500).json({ error: 'O serviço de mensagens RabbitMQ não está pronto.' });
    }

    const { pedido_id, item, mesa } = req.body;
    const dadosPedido = { pedido_id, item, mesa, horario: new Date().toLocaleTimeString() };

    // Publica a mensagem na exchange (ela enviará para a cozinha e para o financeiro)
    channel.publish('pedidos_exchange', '', Buffer.from(JSON.stringify(dadosPedido)));
    
    console.log(`🚀 [PRODUTOR] Pedido #${pedido_id} enviado para o RabbitMQ!`);
    return res.status(201).json({ msg: 'Pedido enfileirado com sucesso!', pedido: dadosPedido });
});

const PORT = 3000;
app.listen(PORT, () => {
    console.log(`🚀 Interface do Cliente rodando em http://localhost:${PORT}`);
});