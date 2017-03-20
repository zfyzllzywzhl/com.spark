package com.manager.untils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.storage.StorageLevel;

import com.github.fommil.netlib.BLAS;
import com.google.common.collect.Sets;






import scala.Tuple2;
/**
 * ����als�Ƽ��㷨�����̽⹹
 * @author ���Ѳ�
 *
 */
public class Recommend  implements Serializable{
	
	// ��ʼ�� Spark
		static SparkConf conf = new SparkConf().setAppName("MovieRecommendation").setMaster("local");
		static JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<Rating> training =null;
		JavaRDD<Rating> validation=null;
		JavaRDD<Rating> test=null;
		JavaRDD<Tuple2<Integer, Rating>> ratings=null;
		Map<Integer, String> products=null;
		
		
		public JavaRDD<Rating> getTraining() {
			return training;
		}

		public void setTraining(JavaRDD<Rating> training) {
			this.training = training;
		}

		public JavaRDD<Rating> getValidation() {
			return validation;
		}

		public void setValidation(JavaRDD<Rating> validation) {
			this.validation = validation;
		}

		public JavaRDD<Rating> getTest() {
			return test;
		}

		public void setTest(JavaRDD<Rating> test) {
			this.test = test;
		}
		

		public JavaRDD<Tuple2<Integer, Rating>> getRatings() {
			return ratings;
		}

		public void setRatings(JavaRDD<Tuple2<Integer, Rating>> ratings) {
			this.ratings = ratings;
		}

		public Map<Integer, String> getProducts() {
			return products;
		}

		public void setProducts(Map<Integer, String> products) {
			this.products = products;
		}

		/**
		 * �����и�����
		 * @param ratingsPath	��������·��
		 * @param moviesPath	��Ʒ����·��
		 */
		public void splitData(String ratingsPath,String moviesPath) {
		// ��������
		final JavaRDD<String> ratingData = sc.textFile(ratingsPath);
		JavaRDD<String> productData = sc.textFile(moviesPath);
		
		//���������и�
		ratings = ratingData.map(new Function<String, Tuple2<Integer, Rating>>() {
			public Tuple2<Integer, Rating> call(String s) throws Exception {
				String[] row = s.split("::");
				Integer cacheStamp = Integer.parseInt(row[3]) % 10;
				Rating rating = new Rating(Integer.parseInt(row[0]), Integer.parseInt(row[1]),
						(float) Double.parseDouble(row[2]));
				return new Tuple2<Integer, Rating>(cacheStamp, rating);
			}
		});

		//��Ʒ�����и�
		 products = productData.mapToPair(new PairFunction<String, Integer, String>() {
			public Tuple2<Integer, String> call(String s) throws Exception {
				String[] sarray = s.split("::");
				return new Tuple2<Integer, String>(Integer.parseInt(sarray[0]), sarray[1]);
			}
		}).collectAsMap();
		 
		 //����������������
		long ratingCount = ratings.count();
		//�����û���������
		long userCount = ratings.map(new Function<Tuple2<Integer, Rating>, Object>() {
			public Object call(Tuple2<Integer, Rating> tuple) throws Exception {
				return tuple._2().user();
			}
		}).distinct().count();
		//������Ʒ��������
		long movieCount = ratings.map(new Function<Tuple2<Integer, Rating>, Object>() {
			public Object call(Tuple2<Integer, Rating> tuple) throws Exception {
				return tuple._2().product();
			}
		}).distinct().count();

		System.out.println(
				"Got " + ratingCount + " ratings from " + userCount + " users on " + movieCount + " products.");
		}
		/*
		 * ѵ������
		 */
		public void trainData(){
		// �����з����ݵĸ���
		int numPartitions = 10;
		// ����ѵ������
		training = ratings.filter(new Function<Tuple2<Integer, Rating>, Boolean>() {
			public Boolean call(Tuple2<Integer, Rating> tuple) throws Exception {
				return tuple._1() < 6;
			}
		}).map(new Function<Tuple2<Integer, Rating>, Rating>() {
			public Rating call(Tuple2<Integer, Rating> tuple) throws Exception {
				return tuple._2();
			}
		}).repartition(numPartitions).cache();

		StorageLevel storageLevel = new StorageLevel();
		// ������֤����
		 validation = ratings.filter(new Function<Tuple2<Integer, Rating>, Boolean>() {
			public Boolean call(Tuple2<Integer, Rating> tuple) throws Exception {
				return tuple._1() >= 6 && tuple._1() < 8;
			}
		}).map(new Function<Tuple2<Integer, Rating>, Rating>() {
			public Rating call(Tuple2<Integer, Rating> tuple) throws Exception {
				return tuple._2();
			}
		}).repartition(numPartitions).persist(storageLevel);

		// ���ò�������
		test = ratings.filter(new Function<Tuple2<Integer, Rating>, Boolean>() {
			public Boolean call(Tuple2<Integer, Rating> tuple) throws Exception {
				return tuple._1() >= 8;
			}
		}).map(new Function<Tuple2<Integer, Rating>, Rating>() {
			public Rating call(Tuple2<Integer, Rating> tuple) throws Exception {
				return tuple._2();
			}
		}).persist(storageLevel);

		long numTraining = training.count();
		long numValidation = validation.count();
		long numTest = test.count();

		System.out.println("Training: " + numTraining + ", validation: " + numValidation + ", test: " + numTest);
		
		}
		
		/**
		 * ѵ�������ģ��
		 * @param training	ѵ������
		 * @param validation	��֤����
		 * @param ranks	��Χ
		 * @param lambdas
		 * @param numIters	��������
		 * @return bestModel	���ģ��
		 */
	//����ģ��δд
	public MatrixFactorizationModel trainModel(JavaRDD<Rating> training,JavaRDD<Rating> validation,int[] ranks,
			float[] lambdas,int[] numIters,String modelSavePath){
		MatrixFactorizationModel sameModel = MatrixFactorizationModel.load(sc.sc(),
				  modelSavePath);
		if(sameModel!=null){
			System.out.println("�����Ѿ�ѵ����ģ��");
			return sameModel;
		}
		else{
			System.out.println("����û��ѵ����ģ��");
			double bestValidationRmse = Double.MAX_VALUE;
		
		int bestRank = 0;
		float bestLambda = -1.0f;
		int bestNumIter = -1;
		MatrixFactorizationModel bestModel = null;

		for (int currentRank : ranks) {
			for (float currentLambda : lambdas) {
				for (int currentNumIter : numIters) {
					MatrixFactorizationModel model = ALS.train(JavaRDD.toRDD(training), currentRank, currentNumIter,
							currentLambda);

					Double validationRmse = computeRMSE(model, validation);
					System.out.println("RMSE (validation) = " + validationRmse + " for the model trained with rank = "
							+ currentRank + ", lambda = " + currentLambda + ", and numIter = " + currentNumIter + ".");

					if (validationRmse < bestValidationRmse) {
						bestModel = model;
						bestValidationRmse = validationRmse;
						bestRank = currentRank;
						bestLambda = currentLambda;
						bestNumIter = currentNumIter;
					}
				}
			}
		}
		//����ģ��
		//SparkContext sc=new SparkContext(conf);
		System.out.println("sc---------------------");
		bestModel.save(sc.sc(), modelSavePath);
		System.out.println("scend---------------------************"+bestModel.logName());
		  
		
		
		return bestModel;
		}
		
	}
	

	
	/**
	 * Calculating the Root Mean Squared Error
	 *
	 * @param model
	 *            best model generated.
	 * @param data
	 *            rating data.
	 * @return Root Mean Squared Error
	 */
	public  Double computeRMSE(MatrixFactorizationModel model, JavaRDD<Rating> data) {
		JavaRDD<Tuple2<Object, Object>> userProducts = data.map(new Function<Rating, Tuple2<Object, Object>>() {
			public Tuple2<Object, Object> call(Rating r) {
				return new Tuple2<Object, Object>(r.user(), r.product());
			}
		});

		JavaPairRDD<Tuple2<Integer, Integer>, Double> predictions = JavaPairRDD
				.fromJavaRDD(model.predict(JavaRDD.toRDD(userProducts)).toJavaRDD()
						.map(new Function<Rating, Tuple2<Tuple2<Integer, Integer>, Double>>() {
							public Tuple2<Tuple2<Integer, Integer>, Double> call(Rating r) {
								return new Tuple2<Tuple2<Integer, Integer>, Double>(
										new Tuple2<Integer, Integer>(r.user(), r.product()), r.rating());
							}
						}));
		JavaRDD<Tuple2<Double, Double>> predictionsAndRatings = JavaPairRDD
				.fromJavaRDD(data.map(new Function<Rating, Tuple2<Tuple2<Integer, Integer>, Double>>() {
					public Tuple2<Tuple2<Integer, Integer>, Double> call(Rating r) {
						return new Tuple2<Tuple2<Integer, Integer>, Double>(
								new Tuple2<Integer, Integer>(r.user(), r.product()), r.rating());
					}
				})).join(predictions).values();

		double mse = JavaDoubleRDD.fromRDD(predictionsAndRatings.map(new Function<Tuple2<Double, Double>, Object>() {
			public Object call(Tuple2<Double, Double> pair) {
				Double err = pair._1() - pair._2();
				return err * err;
			}
		}).rdd()).mean();

		return Math.sqrt(mse);
	}

	/**
	 * �Ը����û��Ƽ���Ʒ
	 *
	 * @param userId �û�ID
	 *            
	 * @param model	���ģ��
	 *           
	 * @param ratings	Ƶ������
	 *            
	 * @param products	��Ʒ�б�
	 *           
	 * @param numResult	�Ƽ�����
	 *            
	 * @return recommendationsResult	�Ƽ����.
	 */
	public  List<Rating> recommendationsResult(final int userId, MatrixFactorizationModel model,
			JavaRDD<Tuple2<Integer, Rating>> ratings, Map<Integer, String> products,int numResult) {
		
		List<Rating> recommendations;
		// ��ȡ�û���������
		JavaRDD<Rating> userRatings = ratings.filter(new Function<Tuple2<Integer, Rating>, Boolean>() {
			public Boolean call(Tuple2<Integer, Rating> tuple) throws Exception {
				return tuple._2().user() == userId;
			}
		}).map(new Function<Tuple2<Integer, Rating>, Rating>() {
			public Rating call(Tuple2<Integer, Rating> tuple) throws Exception {
				return tuple._2();
			}
		});

		// �õ��û����۹��Ĳ�ƷID
		JavaRDD<Tuple2<Object, Object>> userProducts = userRatings.map(new Function<Rating, Tuple2<Object, Object>>() {
			public Tuple2<Object, Object> call(Rating r) {
				return new Tuple2<Object, Object>(r.user(), r.product());
			}
		});

		List<Integer> productSet = new ArrayList<Integer>();
		productSet.addAll(products.keySet());

		Iterator<Tuple2<Object, Object>> productIterator = userProducts.toLocalIterator();

		// ��������Ʒ��ȥ���û����۹��Ĳ�ƷID
		while (productIterator.hasNext()) {
			Integer movieId = (Integer) productIterator.next()._2();
			if (productSet.contains(movieId)) {
				productSet.remove(movieId);
			}
		}

		JavaRDD<Integer> candidates = sc.parallelize(productSet);

		JavaRDD<Tuple2<Integer, Integer>> userCandidates = candidates
				.map(new Function<Integer, Tuple2<Integer, Integer>>() {
					public Tuple2<Integer, Integer> call(Integer integer) throws Exception {
						return new Tuple2<Integer, Integer>(userId, integer);
					}
				});

		// Ϊ�����û�Ԥ���Ƽ���Ʒ
		recommendations = model.predict(JavaPairRDD.fromJavaRDD(userCandidates)).collect();

		// �������ֶ��Ƽ���Ʒ��������
		Collections.sort(recommendations, new Comparator<Rating>() {
			public int compare(Rating r1, Rating r2) {
				return r1.rating() < r2.rating() ? -1 : r1.rating() > r2.rating() ? 1 : 0;
			}
		});

		// ��ȡ����ǰnumResult�Ĳ�Ʒ
		recommendations = recommendations.subList(0, numResult);

		return recommendations;
	}
	// end
	
}
