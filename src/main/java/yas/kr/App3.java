package yas.kr;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class App3 {
    public static void main(String[] args) {
        SparkSession ss = SparkSession.builder()
                .appName("Exo2")
                .master("spark://spark-master:7077")
                .getOrCreate();

        Dataset<Row> df = ss.read().format("csv").option("header", true).load("/bitnami/commands.csv");
        df.createOrReplaceTempView("purchases");

        // Task 1: Afficher le produit le plus vendu en terme de montant total.
        Dataset<Row> totalAmountByProduct = ss.sql("SELECT produit_id, sum(montant) as total_amount FROM purchases GROUP BY produit_id ORDER BY total_amount DESC LIMIT 1");
        totalAmountByProduct.show();

        // Task 2: Afficher les 3 produits les plus vendus dans l'ensemble des données.
        Dataset<Row> top3Products = ss.sql("SELECT produit_id, sum(montant) as total_amount FROM purchases GROUP BY produit_id ORDER BY total_amount DESC LIMIT 3");
        top3Products.show();

        // Task 3: Afficher le montant total des achats pour chaque produit.
        Dataset<Row> totalAmountForEachProduct = ss.sql("SELECT produit_id, sum(montant) as total_amount FROM purchases GROUP BY produit_id ORDER BY produit_id");
        totalAmountForEachProduct.show();

    }
}
