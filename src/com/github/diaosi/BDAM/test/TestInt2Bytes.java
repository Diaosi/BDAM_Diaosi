package com.github.diaosi.BDAM.test;

import java.util.Arrays;

import com.github.diaosi.BDAM.utils.CoOccurenceSingleNode;

public class TestInt2Bytes {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		int i = -128;
		System.out.println(Arrays.toString(CoOccurenceSingleNode.int2bytes(i)));

	}
}
