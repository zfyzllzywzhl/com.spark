package com.manager.bean;

import java.util.List;

public class RecommendResult {
	private int uid;
	private int rid;
	private int rate;

	public RecommendResult(int uid, int rid, int rate, List<RecommendResult> rlist) {
		super();
		this.uid = uid;
		this.rid = rid;
		this.rate = rate;
		this.rlist = rlist;
	}

	private List<RecommendResult> rlist;

	public List<RecommendResult> getRlist() {
		return rlist;
	}

	public void setRlist(List<RecommendResult> rlist) {
		this.rlist = rlist;
	}

	public int getUid() {
		return uid;
	}

	public void setUid(int uid) {
		this.uid = uid;
	}

	public int getRid() {
		return rid;
	}

	public void setRid(int rid) {
		this.rid = rid;
	}

	public int getRate() {
		return rate;
	}

	public void setRate(int rate) {
		this.rate = rate;
	}
}
