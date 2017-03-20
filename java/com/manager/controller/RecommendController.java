package com.manager.controller;

import javax.servlet.http.HttpServletRequest;


import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.servlet.ModelAndView;



@Controller
@RequestMapping(value="/recommend")
public class RecommendController {
	@RequestMapping(value="/index")
	public ModelAndView index(HttpServletRequest request){
		ModelAndView mav=new ModelAndView("/recommend/index");
		return mav;
		
	}
	@RequestMapping(value="/train",method=RequestMethod.POST)
	public ModelAndView train(HttpServletRequest request){
		
		ModelAndView mav=new ModelAndView("/recommend/index");
		return mav;
	}

	
}
