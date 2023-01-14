import java.io.File;
import java.io.*;
import java.util.concurrent.*;

public class Tema2 {
    public static void main(String[] args) throws Exception {
        // parseaza argumentele din linia de comanda
        File orderProductsFile = new File(args[0] + "/order_products.txt");
        File ordersFile = new File(args[0] + "/orders.txt");
        int NUMBER_OF_THREADS = Integer.parseInt(args[1]);

        // creaaza fisierele de iesire
        File reset = new File("order_products_out.txt");
        reset.delete();
        reset = new File("orders_out.txt");
        reset.delete();

        // creeaza un pool cu threaduri de nivel 2
        ExecutorService orderProcessorExecutor = Executors.newFixedThreadPool(NUMBER_OF_THREADS);

        // creeaza un pool cu threaduri de nivel 1
        Thread[] threads = new Thread[NUMBER_OF_THREADS];

        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(
                    new FirstLevelThread(ordersFile.getAbsolutePath(), orderProductsFile.getAbsolutePath(), i,
                            NUMBER_OF_THREADS, orderProcessorExecutor));
            threads[i].start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        orderProcessorExecutor.shutdown();
    }
}

class FirstLevelThread implements Runnable {
    private String ordersFile;
    private String orderProductsFile;
    private String ordersOutputFile;
    private int threadId;
    private int numThreads;
    private ExecutorService orderProcessorExecutor;

    public FirstLevelThread(String ordersFile, String orderProductsFile, int threadId, int numThreads,
            ExecutorService orderProcessorExecutor) {
        this.ordersFile = ordersFile;
        this.orderProductsFile = orderProductsFile;
        this.threadId = threadId;
        this.numThreads = numThreads;
        this.orderProcessorExecutor = orderProcessorExecutor;
        ordersOutputFile = "orders_out.txt";
    }

    @Override
    public void run() {
        try (BufferedReader reader = new BufferedReader(new FileReader(ordersFile))) {
            String line;
            int lineNum = 0;
            while ((line = reader.readLine()) != null) {
                // preia doar comenzile cu cel putin un produs care ii sunt asignate prin ID
                if (lineNum % numThreads == threadId && Integer.parseInt(line.split(",")[1]) > 0) {
                    // CountDownLatch pentru a anunta thread-ul de nivel 1
                    CountDownLatch latch = new CountDownLatch(1);

                    orderProcessorExecutor.submit(new SecondLevelThread(line, orderProductsFile, latch));

                    latch.await();

                    // marcheaza comanda ca shipped
                    synchronized (this) {
                        try {
                            BufferedWriter writer = new BufferedWriter(new FileWriter(ordersOutputFile, true));
                            writer.write(line + "," + "shipped\n");
                            writer.close();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
                lineNum++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

class SecondLevelThread implements Runnable {
    private String line;
    private String orderProductsFile;
    private String orderProductsOutputFile;
    private CountDownLatch latch;

    public SecondLevelThread(String line, String orderProductsFile, CountDownLatch latch) {
        this.line = line;
        this.orderProductsFile = orderProductsFile;
        this.latch = latch;
        orderProductsOutputFile = "order_products_out.txt";
    }

    @Override
    public void run() {
        // parseaza linia de text
        String[] lineParts = line.split(",");
        String orderID = lineParts[0];
        int productNumber = Integer.parseInt(lineParts[1]);

        try (BufferedReader reader = new BufferedReader(new FileReader(orderProductsFile))) {
            String readLine;
            while ((readLine = reader.readLine()) != null) {
                if (readLine.split(",")[0].equals(orderID)) {
                    // marcheaza produsul ca "shipped" din comanda ce trebuie procesata
                    synchronized (this) {
                        try {
                            BufferedWriter writer = new BufferedWriter(new FileWriter(orderProductsOutputFile, true));
                            writer.write(readLine + "," + "shipped\n");
                            writer.close();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    productNumber--;
                
                    // anunta thread-ul de nivel 1 ca produsele au fost procesate
                    if (productNumber <= 0) {
                        latch.countDown();
                        break;
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
