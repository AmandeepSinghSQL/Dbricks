# Databricks notebook source
catalogs = spark.sql("SHOW CATALOGS")
display(catalogs)

