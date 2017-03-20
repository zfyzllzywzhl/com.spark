 package com.manager.untils;

import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;

import com.manager.bean.User;

public class Test {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String ratingsPath="hdfs://172.18.16.237:9000//user/root//ratings.dat";
		String moviesPath="hdfs://172.18.16.237:9000//user/root//movies.dat";
		int[] ranks = { 8, 12 };
		float[] lambdas = { 0.1f, 10.0f };
		int[] numIters = { 10, 20 };
		int userId=4;
		int numResult=4;
		String modelSavePath="hdfs://172.18.16.237:9000//user/fansy";
		Recommend recommend=new Recommend();
		//1.�����и�����
		recommend.splitData(ratingsPath, moviesPath);
		//2.ѵ������
		//recommend.trainData();
		//3.ѵ��ģ��
		//MatrixFactorizationModel bestModel=recommend.trainModel(recommend.getTraining(), recommend.getValidation(), ranks, lambdas, numIters,modelSavePath);
		//System.out.println("ѵ����ģ��Ϊ"+bestModel.logName());
		//4.����RMSE
		//recommend.computeRMSE(bestModel, recommend.getTest());
		//5.�Ƽ����
		MatrixFactorizationModel sameModel = MatrixFactorizationModel.load(recommend.sc.sc(),
				modelSavePath);
		System.out.println("ģ��"+sameModel);
		
		
		System.out.println("���Ϊ��"+recommend.recommendationsResult(userId, sameModel, recommend.getRatings(), recommend.getProducts(), numResult));
	}

}
