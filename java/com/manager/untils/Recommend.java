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
 * 基于als推荐算法的流程解构
 * @author 许友昌
 *
 */
public class Recommend  implements Serializable{
	
	// 初始化 Spark
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
		 * 导入切割数据
		 * @param ratingsPath	评分数据路径
		 * @param moviesPath	物品数据路径
		 */
		public void splitData(String ratingsPath,String moviesPath) {
		// 导入数据
		final JavaRDD<String> ratingData = sc.textFile(ratingsPath);
		JavaRDD<String> productData = sc.textFile(moviesPath);
		
		//评分数据切割
		ratings = ratingData.map(new Function<String, Tuple2<Integer, Rating>>() {
			public Tuple2<Integer, Rating> call(String s) throws Exception {
				String[] row = s.split("::");
				Integer cacheStamp = Integer.parseInt(row[3]) % 10;
				Rating rating = new Rating(Integer.parseInt(row[0]), Integer.parseInt(row[1]),
						(float) Double.parseDouble(row[2]));
				return new Tuple2<Integer, Rating>(cacheStamp, rating);
			}
		});

		//物品数据切割
		 products = productData.mapToPair(new PairFunction<String, Integer, String>() {
			public Tuple2<Integer, String> call(String s) throws Exception {
				String[] sarray = s.split("::");
				return new Tuple2<Integer, String>(Integer.parseInt(sarray[0]), sarray[1]);
			}
		}).collectAsMap();
		 
		 //计算评分数据总量
		long ratingCount = ratings.count();
		//计算用户数据重量
		long userCount = ratings.map(new Function<Tuple2<Integer, Rating>, Object>() {
			public Object call(Tuple2<Integer, Rating> tuple) throws Exception {
				return tuple._2().user();
			}
		}).distinct().count();
		//计算物品数据总量
		long movieCount = ratings.map(new Function<Tuple2<Integer, Rating>, Object>() {
			public Object call(Tuple2<Integer, Rating> tuple) throws Exception {
				return tuple._2().product();
			}
		}).distinct().count();

		System.out.println(
				"Got " + ratingCount + " ratings from " + userCount + " users on " + movieCount + " products.");
		}
		/*
		 * 训练数据
		 */
		public void trainData(){
		// 设置切分数据的个数
		int numPartitions = 10;
		// 设置训练数据
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
		// 设置验证数据
		 validation = ratings.filter(new Function<Tuple2<Integer, Rating>, Boolean>() {
			public Boolean call(Tuple2<Integer, Rating> tuple) throws Exception {
				return tuple._1() >= 6 && tuple._1() < 8;
			}
		}).map(new Function<Tuple2<Integer, Rating>, Rating>() {
			public Rating call(Tuple2<Integer, Rating> tuple) throws Exception {
				return tuple._2();
			}
		}).repartition(numPartitions).persist(storageLevel);

		// 设置测试数据
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
		 * 训练出最佳模型
		 * @param training	训练数据
		 * @param validation	验证数据
		 * @param ranks	范围
		 * @param lambdas
		 * @param numIters	迭代次数
		 * @return bestModel	最佳模型
		 */
	//保存模型未写
	public MatrixFactorizationModel trainModel(JavaRDD<Rating> training,JavaRDD<Rating> validation,int[] ranks,
			float[] lambdas,int[] numIters,String modelSavePath){
		MatrixFactorizationModel sameModel = MatrixFactorizationModel.load(sc.sc(),
				  modelSavePath);
		if(sameModel!=null){
			System.out.println("进入已经训练的模型");
			return sameModel;
		}
		else{
			System.out.println("进入没有训练的模型");
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
		//保存模型
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
	 * 对给出用户推荐产品
	 *
	 * @param userId 用户ID
	 *            
	 * @param model	最佳模型
	 *           
	 * @param ratings	频分数据
	 *            
	 * @param products	物品列表
	 *           
	 * @param numResult	推荐个数
	 *            
	 * @return recommendationsResult	推荐结果.
	 */
	public  List<Rating> recommendationsResult(final int userId, MatrixFactorizationModel model,
			JavaRDD<Tuple2<Integer, Rating>> ratings, Map<Integer, String> products,int numResult) {
		
		List<Rating> recommendations;
		// 获取用户评分数据
		JavaRDD<Rating> userRatings = ratings.filter(new Function<Tuple2<Integer, Rating>, Boolean>() {
			public Boolean call(Tuple2<Integer, Rating> tuple) throws Exception {
				return tuple._2().user() == userId;
			}
		}).map(new Function<Tuple2<Integer, Rating>, Rating>() {
			public Rating call(Tuple2<Integer, Rating> tuple) throws Exception {
				return tuple._2();
			}
		});

		// 得到用户评价过的产品ID
		JavaRDD<Tuple2<Object, Object>> userProducts = userRatings.map(new Function<Rating, Tuple2<Object, Object>>() {
			public Tuple2<Object, Object> call(Rating r) {
				return new Tuple2<Object, Object>(r.user(), r.product());
			}
		});

		List<Integer> productSet = new ArrayList<Integer>();
		productSet.addAll(products.keySet());

		Iterator<Tuple2<Object, Object>> productIterator = userProducts.toLocalIterator();

		// 从所有物品中去除用户评价过的产品ID
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

		// 为给出用户预测推荐产品
		recommendations = model.predict(JavaPairRDD.fromJavaRDD(userCandidates)).collect();

		// 根据评分对推荐产品进行排序
		Collections.sort(recommendations, new Comparator<Rating>() {
			public int compare(Rating r1, Rating r2) {
				return r1.rating() < r2.rating() ? -1 : r1.rating() > r2.rating() ? 1 : 0;
			}
		});

		// 获取排名前numResult的产品
		recommendations = recommendations.subList(0, numResult);

		return recommendations;
	}
	// end
	
}
