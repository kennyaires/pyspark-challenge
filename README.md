# pyspark-challenge
Operações de big data do framework Apache Spark utilizando a biblioteca pyspark.


# questões

**Qual o objetivo do comando cache em Spark?**
A função do cache evita que haja repetitivos acessos ao disco em busca de recursos utilizados constantemente no ambiente Spark, carregando-os na memória para um rápido acesso. Por padrão, os RDD's são carregados de forma *lazy* - isto é, uma vez instanciado o RDD é apenas uma abstração de instruções para a sua manipulação -, somente ao chamar uma ação, há de fato uma coleta de dados do RDD instanciado anteriormente, o que é morado, e uma vez que pode-se reutilizar esse resultado no restante da aplicação, convém usar o cache para melhorar a performance.

**O mesmo código implementado em Spark é normalmente mais rápido que a implementação equivalente em MapReduce. Por quê?**
Isso se deve à principal diferença em termos performáticos dos dois frameworks: o Spark executa processamento em memória RAM enquanto que o MapReduce possui escrita e leitura em disco rígido, que é comparavelmente muito mais lento.

**Qual é a função do SparkContext?**
Trata-se de um objeto que é a instancialização de qualquer aplicação Spark, esse objeto possibilita o cliente a se conectar à um cluster Spark e criar RDD's seguindo as configurações passadas no momento da instancialização.

**Explique com suas palavras o que é Resilient​ ​Distributed​ ​Datasets​ (RDD).**
São as estruturas de dados do framework Spark. RDD é um acrônimo para *Resilient Distributed Datasets*, é então uma coleção de dados tolerantes à falha que podem ser executadas em paralelo. Sua instância é por definição *lazy*, nela é carregada uma série de instruções para ações e transfomações para manipulação dos dados - como por exemplo as operações `map` e `reduce`.

**GroupByKey é menos eficiente que reduceByKey em grandes dataset. Por quê?**
O algoritmo do `GroupByKey` transfere mais dados em suas operações do que o `reduceByKey`. A eficiência da função `reduceByKey` vem do fato de que, a cada operação de elementos com a mesma chave, é computado em seguida um resultado parcial, antes de ser passada a execução para o resultado final. No caso do `GroupByKey`, não há o calculo de resultados parciais, o que faz carregar na memória muito mais dados para só então realizar a operação final. 

**Explique o que o código Scala abaixo faz.**
```
1.  val textFile = sc.textFile("hdfs://...")
2.  val counts = textFile.flatMap(line => line.split(" "))
3.  .map(word => (word, 1))
4.  .reduceByKey(_ + _)
5.  counts.saveAsTextFile("hdfs://...")
```

Na primeira linha, o RDD É instanciado a partir de um arquivo no ambiente da aplicação Spark. Na segunda linha, há o tratamento dos dados vindos do arquivo de texto, e então na linha seguinte, os dados são preparados para serem contabilizados - cada palavra, uma ocorrência -, e finalmente na quarta linha a operação de contabilizar por chave é realizada - reduzindo as repetidas ocorrências de uma palavra em um único elemento. Na linha 5 o resultado da operação é salvo em um arquivo no endereço especificado. 