const amqp = require('amqplib');

async function iniciarFinanceiro() {
    try {
        const connection = await amqp.connect('amqp://localhost:5672');
        const channel = await connection.createChannel();

        await channel.assertExchange('pedidos_exchange', 'fanout', { durable: true });
        
        // Cria a fila específica do financeiro/pagamento
        const q = await channel.assertQueue('fila_pagamento', { durable: true });
        await channel.bindQueue(q.queue, 'pedidos_exchange', '');

        console.log('📥 [FINANCEIRO] Aguardando pedidos na "fila_pagamento"... (Pressione CTRL+C para parar)');

        channel.consume(q.queue, (msg) => {
            if (msg !== null) {
                const pedido = JSON.parse(msg.content.toString());
                
                console.log(`\n💳 [FINANCEIRO] Processando pagamento do Pedido #${pedido.pedido_id}`);
                console.log(`   ↳ Valor faturado para a Mesa: ${pedido.mesa}`);
                
                // Simula o processamento do pagamento em 1.5 segundos
                setTimeout(() => {
                    console.log(`💵 [FINANCEIRO] Cobrança do Pedido #${pedido.pedido_id} Aprovada!`);
                    channel.ack(msg);
                }, 30000);
            }
        }, { noAck: false });

    } catch (error) {
        console.error('❌ Erro no Consumidor do Financeiro:', error);
    }
}

iniciarFinanceiro();