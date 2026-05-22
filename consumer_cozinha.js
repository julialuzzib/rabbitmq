const amqp = require('amqplib');

async function iniciarCozinha() {
    try {
        const connection = await amqp.connect('amqp://localhost:5672');
        const channel = await connection.createChannel();

        // Garante que a exchange existe
        await channel.assertExchange('pedidos_exchange', 'fanout', { durable: true });
        
        // Cria a fila específica da cozinha
        const q = await channel.assertQueue('fila_cozinha', { durable: true });
        
        // Vincula a fila à exchange de pedidos
        await channel.bindQueue(q.queue, 'pedidos_exchange', '');

        console.log('📥 [COZINHA] Aguardando novos pedidos na "fila_cozinha"... (Pressione CTRL+C para parar)');

        // Consome as mensagens da fila
        channel.consume(q.queue, (msg) => {
            if (msg !== null) {
                const pedido = JSON.parse(msg.content.toString());
                
                console.log(`\n🍳 [COZINHA] Preparando Pedido #${pedido.pedido_id}`);
                console.log(`   ↳ Item: ${pedido.item} | Mesa: ${pedido.mesa}`);
                
                // Simula o tempo de preparo de 3 segundos
                setTimeout(() => {
                    console.log(`✅ [COZINHA] Pedido #${pedido.pedido_id} Finalizado!`);
                    channel.ack(msg); // Confirma para o RabbitMQ que a mensagem foi processada com sucesso
                }, 45000);
            }
        }, { noAck: false }); // noAck: false garante que se a cozinha cair durante o preparo, o pedido volta para a fila

    } catch (error) {
        console.error('❌ Erro no Consumidor da Cozinha:', error);
    }
}

iniciarCozinha();