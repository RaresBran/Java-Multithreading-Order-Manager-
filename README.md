Bran Rares 331CC

Programul foloseste elemente de multithreading pentru a rezolva problema.

In primul rand se creeza P thread-uri de nivel 1 pentru a citi fisierul orders.txt. Acesta este impartit la fiecare thread in functie de ID-ul sau. De asemenea se creeaza un ExecutorService pentru Thread-urile de nivel 2.

Fiecare thread de nivel 1 citeste o comanda si creeaza un task pentru orderProcessorExecutor. Un thread de nivel 2 disponibil preia task-ul si citeste fisierul order_products.txt actualizand in fisierul de iesire fiecare produs ce face parte din comanda prelucrata cu tag-ul "shipped". Printr-un CountDownLatch, thread-ul de nivel 2 semnaleaza thread-ul de nivel 1 ca a fost prelucrata comanda ca acesta sa poata marca comanda cu "shipped" in fisierul de iesire.