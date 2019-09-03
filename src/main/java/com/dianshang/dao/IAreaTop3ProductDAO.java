package com.dianshang.dao;

import com.dianshang.domain.AreaTop3Product;

import java.util.List;


/**
 * 各区域top3热门商品DAO接口
 * @author Administrator
 *
 */
public interface IAreaTop3ProductDAO {

	void insertBatch(List<AreaTop3Product> areaTopsProducts);
	
}
