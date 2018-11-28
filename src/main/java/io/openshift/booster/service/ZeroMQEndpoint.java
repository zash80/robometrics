package io.openshift.booster.service;

import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.util.Random;

@Component
public class ZeroMQEndpoint {

    private static Random rand = new Random(System.nanoTime());

    @EventListener(ApplicationReadyEvent.class)
    public void appReady() {
        System.err.println("Application Ready...");

        new Thread(()->{

            try (ZContext ctx = new ZContext()) {
                //  Frontend socket talks to clients over TCP
                ZMQ.Socket frontend = ctx.createSocket(ZMQ.ROUTER);
                frontend.bind("tcp://*:5555");

                //  Backend socket talks to workers over inproc
                ZMQ.Socket backend = ctx.createSocket(ZMQ.DEALER);
                backend.bind("inproc://backend");

                //  Launch pool of worker threads, precise number is not critical
                for (int threadNbr = 0; threadNbr < 5; threadNbr++)
                    new Thread(() -> {


                        ZMQ.Socket worker = ctx.createSocket(ZMQ.DEALER);
                        worker.connect("inproc://backend");

                        while (!Thread.currentThread().isInterrupted()) {
                            //  The DEALER socket gives us the address envelope and message
                            ZMsg msg = ZMsg.recvMsg(worker);
                            ZFrame address = msg.pop();
                            ZFrame content = msg.pop();
                            assert (content != null);
                            msg.destroy();

                            //  Send 0..4 replies back
                            int replies = rand.nextInt(5);
                            for (int reply = 0; reply < replies; reply++) {
                                //  Sleep for some fraction of a second
                                try {
                                    Thread.sleep(rand.nextInt(1000) + 1);
                                }
                                catch (InterruptedException e) {
                                }
                                address.send(worker, ZFrame.REUSE + ZFrame.MORE);
                                content.send(worker, ZFrame.REUSE);
                            }
                            address.destroy();
                            content.destroy();
                        }
                        ctx.destroy();

                    }).start();

                //  Connect backend to frontend via a proxy
                ZMQ.proxy(frontend, backend, null);
            }

        }).start();
    }
}

