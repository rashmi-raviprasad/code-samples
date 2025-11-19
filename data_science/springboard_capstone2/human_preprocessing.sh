#!/bin/bash

for dir in faces_dataset/download/*
do

echo "$dir"
mv "$dir"/face/* FINAL_faces/

done