import org.apache.spark.{SparkContext, SparkConf}


// author : Mohamed Adel Hassan

object SparkExample extends Serializable {

  def main(args: Array[String]): Unit = {

    
    val conf = new SparkConf().setAppName("spark linkedin-data-analysis").setMaster("local")
    val sc = new SparkContext(conf)
    
    val dataset = sc.textFile("hdfs://localhost:9000/Linkedin/myConnections.csv")
    val header = dataset.first()

    /*                          skipping header line in csv file                        */

    val datasetWithNoHeader = dataset.filter(!_.contains(header))


    /*                         Filter people who has data Fields                       */

    val dataPositionsKeys = List("data analysis", "data engineer", "big data", "data scientist"
      , "business intelligence", "bi analyst", "data analyst", "data science", "data management", "data integration"
      , "database"
    )

    val peopleHasDataPosition = datasetWithNoHeader
      .filter(line => dataPositionsKeys.exists(line.split(",")(4).toLowerCase().contains(_)))


    /*          count peopleHasDataPosition for each company sorted by desc                */

    val companiesHiringDataPositions = peopleHasDataPosition
      .filter(_.split(",")(3) != "")
      .map(_.split(",")(3))
      .map((_, 1))
      .reduceByKey(_ + _)
      .map(tuple => (tuple._2, tuple._1))
      .sortByKey(false)


    companiesHiringDataPositions.collect().foreach(println)

    /*
       (19,Vodafone)
       (15,Upwork)
       (12,IBM)
       (7,Freelance)
       (6,CAT Reloaded)
       (6,Freelancer.com)
       (5,EJADA)
       (5,Telecom Egypt)

    */


    /*                  drop all records contains null values                                         */

    val dropAnyNullValues = datasetWithNoHeader.filter(line => {
      val person = line.split(",")
      !person.contains("")

    })

    case class Person(firstname: String, lastname: String
                      , email_address: String, company: String
                      , position: String, dateofConnected: String)



    /*                   parsing each line to Person object                                  */

    val arrayOfPerson = dropAnyNullValues.map(line =>
      Person(line.split(",")(0), line.split(",")(1), line.split(",")(2), line.split(",")(3), line.split(",")(4),
        line.split(",")(5)
      )
    )


    //arrayOfPerson.collect().foreach(println)
    /* samples of outputs

       Person(Ahmed,Naseem,ahmed_naseem1@outlook.com,Concept Creator,Creative Designer,20 Mar 2021)
       Person(Muhammad,Jamal,muhammadjammi18@gmail.com,Upwork,Frontend Web Developer,15 Feb 2021)
       .........................................................................................
       .........................................................................................

    */


    /*                  get numbers of connections for each position                                   */

    val peopleHasPosition = datasetWithNoHeader.filter(_.split(",")(4) != "")

    val positionCounts = peopleHasPosition
      .map(record => (record.split(",")(4).toLowerCase, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)

    /* positionCounts.collect().foreach(println)

       (data analyst,44)
       (data engineer,34)
       (software engineer,34)
       (data scientist,26)
       (big data engineer,19)
       (student,16)
       (android developer,15)
       (machine learning engineer,12)
     */


    val myRdd = datasetWithNoHeader.map(record =>
      (record.split(",")(0) + " " + record.split(",")(1)
        , record.split(",")(record.split(",").length - 1)
      ))

    //myRdd.collect().foreach(println)

    /*

      (Anas Esam,08 Nov 2020)
      (Mohamed Taha,07 Nov 2020)
      (hossam elden abdelhamed,07 Nov 2020)
      (Mahmoud Khalil,07 Nov 2020)
      ............................

     */


    val countConnectionsByYear = datasetWithNoHeader
      .map(record => {
        val last_index = record.split(",").length - 1
        val dateConnectedOn = record.split(",")(last_index)
        (dateConnectedOn.split(" ")(2), 1)

      })
      .reduceByKey(_ + _)
      .sortBy(_._2, false)


     // countConnectionsByYear.collect().foreach(println)

     /*
         (2021,488)
         (2020,480)
         (2019,477)
         ..........
         ..........

     */


    val countByYear = peopleHasDataPosition.map(record => {
      val last_index = record.split(",").length - 1
      val dateConnectedOn = record.split(",")(last_index)
      (dateConnectedOn.split(" ")(2), 1)

    })
      .reduceByKey(_ + _)
      .sortBy(_._2, false)

    // countByYear.collect().foreach(println)

    /*
         (2020,154)
         (2021,86)
         (2019,3)
         (2018,1)
         ........
         ........

     */

  }

}
