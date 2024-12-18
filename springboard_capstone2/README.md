# Capstone 2: Hybrid Face Generator

This project is the second capstone for the Springboard Data Science course. The goal was to see if a variational autoencoder is able to learn features of cat and human faces in order to create combined faces. This project was inspired by and modeled after CodeParade's [YouTube video](https://www.youtube.com/watch?v=4VAkrUNLKSo&t=3s) and [source code](https://github.com/HackerPoet/FaceEditor), where they generate unique faces based on images taken from a high school yearbook.

## Steps

This project consisted of five stages:

1. Data Acquisition - [cat dataset](https://www.kaggle.com/crawford/cat-dataset/data) was downloaded from Kaggle and [face dataset](http://vintage.winklerbros.net/facescrub.html) was requested from principal investigators who previously prepared the data for a paper<sup name="a1">[1](#f1)</sup> 
2. Data Cleaning - [cropping cat images](https://github.com/rashmi-raviprasad/springboard/blob/master/faces_capstone2/cat_preprocessing.py) based on provided annotations, [writing shell script](https://github.com/rashmi-raviprasad/springboard/blob/master/faces_capstone2/human_preprocessing.sh) to move images, and [resizing images](https://github.com/rashmi-raviprasad/springboard/blob/master/faces_capstone2/resize.ipynb)
3. Model Building and Training - first a [large scale model](https://github.com/rashmi-raviprasad/springboard/blob/master/faces_capstone2/faces_decoder_large.ipynb), then a [reduced model](https://github.com/rashmi-raviprasad/springboard/blob/master/faces_capstone2/faces_decoder_reduced.ipynb); periodically [backing up project files](https://github.com/rashmi-raviprasad/springboard/blob/master/faces_capstone2/push_to_git.ipynb)
4. Creating Application - [making an interface](https://github.com/rashmi-raviprasad/springboard/blob/master/faces_capstone2/application.py) to allow user to adjust sliders that controlled the latent space vectors
5. Writing Final Report - [visualizing model performance](https://github.com/rashmi-raviprasad/springboard/blob/master/faces_capstone2/model_performance.ipynb), creating [final write-up](https://github.com/rashmi-raviprasad/springboard/blob/master/faces_capstone2/Capstone%202%20Final%20Report.pdf) of project, and making [slideshow](https://github.com/rashmi-raviprasad/springboard/blob/master/faces_capstone2/Capstone%202%20Presentation.pptx)

<sup><b name="f1">1</b></sup>H.-W. Ng, S. Winkler. [A data-driven approach to cleaning large face datasets](http://vintage.winklerbros.net/Publications/icip2014a.pdf). Proc. IEEE International Conference on Image Processing (ICIP), Paris, France, Oct. 27-30, 2014.[↩](#a1)
