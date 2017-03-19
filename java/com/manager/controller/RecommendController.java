package com.manager.controller;

import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;

import com.manager.bean.User;
import com.manager.untils.Recommend;

//save model
public class RecommendController {

	public static void main(String[] args){
		String ratingsPath="D://谷歌下载//ml-1m//ratings.dat";
		String moviesPath="D://谷歌下载//ml-1m//movies.dat";
		int[] ranks = { 8, 12 };
		float[] lambdas = { 0.1f, 10.0f };
		int[] numIters = { 10, 20 };
		int numResult=4;
		//User u=new User();
		int userId=4;
		 test( ratingsPath, moviesPath, ranks, lambdas, numIters, userId,numResult);
	}
	public static void test(String ratingsPath,String moviesPath,int[] ranks,float[] lambdas,int[] numIters,int userId,int numResult){
		Recommend recommend=new Recommend();
		MatrixFactorizationModel bestModel=null;
		//1.加载切割数据
		recommend.splitData(ratingsPath, moviesPath);
		//2.训练模型
		bestModel=recommend.trainModel(recommend.getTraining(), recommend.getValidation(), ranks, lambdas, numIters);
		//3.计算bestModel的RMSE
		recommend.computeRMSE(bestModel, recommend.getTest());
		//4.推荐结果
		System.out.println("结果："+recommend.recommendationsResult(userId, bestModel, recommend.getRatings(),recommend.getProducts(),numResult));
	}
}
