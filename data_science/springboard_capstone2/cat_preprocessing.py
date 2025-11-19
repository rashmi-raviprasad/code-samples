import os
import cv2
import numpy as np
import matplotlib.pyplot as plt

#Defining a class which has as attributes the images and annotations
#of each cat picture. Member functions will be used to perform
#the preprocessing.
class CatPic:
    def __init__(self, name):
        '''
        Initializes CatPic class with the cat's 'name' (a portion of the filename)
        '''
        self.name = name
        
    def add_image(self, filename):
        '''
        Assigns image to CatPic from file
        '''
        img = cv2.imread(filename)
        self.rows, self.cols = img.shape[:2]
        self.image = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
    
    def add_coords(self, coord_dict):
        '''
        Assigns annotations to CatPic from dict of coordinates taken from file
        '''
        self.x_l_eye = coord_dict['x_l_eye']
        self.y_l_eye = coord_dict['y_l_eye']
        self.x_r_eye = coord_dict['x_r_eye']
        self.y_r_eye = coord_dict['y_r_eye']
        self.x_mouth = coord_dict['x_mouth']
        self.y_mouth = coord_dict['y_mouth']
        self.x_l_ear1 = coord_dict['x_l_ear1']
        self.y_l_ear1 = coord_dict['y_l_ear1']
        self.x_l_ear2 = coord_dict['x_l_ear2']
        self.y_l_ear2 = coord_dict['y_l_ear2']
        self.x_l_ear3 = coord_dict['x_l_ear3']
        self.y_l_ear3 = coord_dict['y_l_ear3']
        self.x_r_ear1 = coord_dict['x_r_ear1']
        self.y_r_ear1 = coord_dict['y_r_ear1']
        self.x_r_ear2 = coord_dict['x_r_ear2']
        self.y_r_ear2 = coord_dict['y_r_ear2']
        self.x_r_ear3 = coord_dict['x_r_ear3']
        self.y_r_ear3 = coord_dict['y_r_ear3']
    
    def rotate_point(self, x, y):
        '''
        Rotates point by specified angle about specified point
        '''
        c_x, c_y = self.rotation_center
        rad = np.deg2rad(self.theta)
        new_x = c_x + np.cos(rad) * (x - c_x) + np.sin(rad) * (y - c_y)
        new_y = c_y + -np.sin(rad) * (x - c_x) + np.cos(rad) * (y - c_y)
        return new_x, new_y
        
    def transform(self, flip=False):
        '''
        Flips image & annotation coordinates if rotation angle is negative, 
        then rotates image & annotation coordinates by rotation angle so that
        cat's eyes are parallel
        '''
        if flip:
            self.flip_image = cv2.flip(self.image, 1)
            self.flip_x_l_eye = self.cols - self.x_l_eye
            self.flip_x_r_eye = self.cols - self.x_r_eye
            self.flip_x_mouth = self.cols - self.x_mouth
            self.flip_x_l_ear1 = self.cols - self.x_l_ear1
            self.flip_x_l_ear2 = self.cols - self.x_l_ear2
            self.flip_x_l_ear3 = self.cols - self.x_l_ear3
            self.flip_x_r_ear1 = self.cols - self.x_r_ear1
            self.flip_x_r_ear2 = self.cols - self.x_r_ear2
            self.flip_x_r_ear3 = self.cols - self.x_r_ear3
            
            eye_slope = (self.y_l_eye - self.y_r_eye) / (self.flip_x_l_eye - self.flip_x_r_eye)
            self.theta = np.rad2deg(np.arctan(eye_slope))
            self.rotation_center = ((self.y_l_eye + self.y_r_eye)/2, (self.flip_x_l_eye + self.flip_x_r_eye)/2)
            M = cv2.getRotationMatrix2D(self.rotation_center, self.theta, scale=1)
            self.rot_image = cv2.warpAffine(self.flip_image, M, (self.cols, self.rows))
            self.rot_x_l_eye, self.rot_y_l_eye = self.rotate_point(self.flip_x_l_eye, self.y_l_eye)
            self.rot_x_r_eye, self.rot_y_r_eye = self.rotate_point(self.flip_x_r_eye, self.y_r_eye)
            self.rot_x_mouth, self.rot_y_mouth = self.rotate_point(self.flip_x_mouth, self.y_mouth)
            self.rot_x_l_ear1, self.rot_y_l_ear1 = self.rotate_point(self.flip_x_l_ear1, self.y_l_ear1)
            self.rot_x_l_ear2, self.rot_y_l_ear2 = self.rotate_point(self.flip_x_l_ear2, self.y_l_ear2)
            self.rot_x_l_ear3, self.rot_y_l_ear3 = self.rotate_point(self.flip_x_l_ear3, self.y_l_ear3)
            self.rot_x_r_ear1, self.rot_y_r_ear1 = self.rotate_point(self.flip_x_r_ear1, self.y_r_ear1)
            self.rot_x_r_ear2, self.rot_y_r_ear2 = self.rotate_point(self.flip_x_r_ear2, self.y_r_ear2)
            self.rot_x_r_ear3, self.rot_y_r_ear3 = self.rotate_point(self.flip_x_r_ear3, self.y_r_ear3)
        
        else:
            eye_slope = (self.y_l_eye - self.y_r_eye) / (self.x_l_eye - self.x_r_eye)
            self.theta = np.rad2deg(np.arctan(eye_slope))
            self.rotation_center = ((self.y_l_eye + self.y_r_eye)/2, (self.x_l_eye + self.x_r_eye)/2)
            M = cv2.getRotationMatrix2D(self.rotation_center, self.theta, scale=1)
            self.rot_image = cv2.warpAffine(self.image, M, (self.cols, self.rows))
            self.rot_x_l_eye, self.rot_y_l_eye = self.rotate_point(self.x_l_eye, self.y_l_eye)
            self.rot_x_r_eye, self.rot_y_r_eye = self.rotate_point(self.x_r_eye, self.y_r_eye)
            self.rot_x_mouth, self.rot_y_mouth = self.rotate_point(self.x_mouth, self.y_mouth)
            self.rot_x_l_ear1, self.rot_y_l_ear1 = self.rotate_point(self.x_l_ear1, self.y_l_ear1)
            self.rot_x_l_ear2, self.rot_y_l_ear2 = self.rotate_point(self.x_l_ear2, self.y_l_ear2)
            self.rot_x_l_ear3, self.rot_y_l_ear3 = self.rotate_point(self.x_l_ear3, self.y_l_ear3)
            self.rot_x_r_ear1, self.rot_y_r_ear1 = self.rotate_point(self.x_r_ear1, self.y_r_ear1)
            self.rot_x_r_ear2, self.rot_y_r_ear2 = self.rotate_point(self.x_r_ear2, self.y_r_ear2)
            self.rot_x_r_ear3, self.rot_y_r_ear3 = self.rotate_point(self.x_r_ear3, self.y_r_ear3)
        
    def crop_w_border(self):
        '''
        Defines bounding box containing cat's face and crops image.
        Pads image first if bounding box is out of range
        '''
        rot_rows, rot_cols = self.rot_image.shape[:2]
        ear_mouth_height = self.rot_y_mouth - min([self.rot_y_l_ear2, self.rot_y_r_ear2])
        bbox_y_min = int(min([self.rot_y_l_ear2, self.rot_y_r_ear2]) - 0.1 * ear_mouth_height)
        bbox_y_max = int(self.rot_y_mouth + 0.2 * ear_mouth_height)
        bbox_size = bbox_y_max - bbox_y_min
        vert_center = 0.5 * (self.rot_x_l_ear2 + self.rot_x_r_ear2)
        bbox_x_min = int(vert_center - 0.5 * bbox_size)
        bbox_x_max = int(vert_center + 0.5 * bbox_size)
        padding = [0]
        if bbox_y_min < 0:
            padding.append(abs(bbox_y_min))
        if bbox_x_min < 0:
            padding.append(abs(bbox_x_min))
        if bbox_y_max > rot_rows:
            padding.append(bbox_y_max - rot_rows)
        if bbox_x_max > rot_cols:
            padding.append(bbox_x_max - rot_cols)
        
        max_padding = max(padding)
        padding_color = [0, 0, 0]
        pad_image = cv2.copyMakeBorder(self.rot_image, max_padding, max_padding, max_padding, max_padding,
                                      cv2.BORDER_CONSTANT, value=padding_color)
        
        new_y_min, new_y_max = bbox_y_min + max_padding, bbox_y_max + max_padding
        new_x_min, new_x_max = bbox_x_min + max_padding, bbox_x_max + max_padding
        self.cropped_image = pad_image[new_y_min:new_y_max, new_x_min:new_x_max]
        
    def save_to_file(self, directory):
        '''
        Saves processed image to file in jpg format
        '''
        filename = directory + self.name + '.jpg'
        cv2.imwrite(filename, cv2.cvtColor(self.cropped_image, cv2.COLOR_RGB2BGR))

def run(file_directory):
    '''
    Cleans up all cat images in a given directory
    '''
    files = []
    for file in os.listdir(file_directory):
        if file.endswith('.jpg'):
            files.append(file)
    names = [f.split('.')[-2][4:] for f in files]
    pic_dict = {}
    coord_list = ['x_l_eye', 'y_l_eye', 'x_r_eye', 'y_r_eye', 'x_mouth', 'y_mouth',
                'x_l_ear1', 'y_l_ear1', 'x_l_ear2', 'y_l_ear2', 'x_l_ear3', 'y_l_ear3',
                'x_r_ear1', 'y_r_ear1', 'x_r_ear2', 'y_r_ear2', 'x_r_ear3', 'y_r_ear3']

    for i, f in enumerate(files):
        pic = CatPic(names[i])
        pic.add_image(file_directory + f)
        with open(file_directory + f + '.cat') as annotation_file:
            for line in annotation_file:
                coord_vals = [int(l) for l in line.split()[1:]]
                coord_dict = dict(zip(coord_list, coord_vals))
                pic.add_coords(coord_dict)
        pic_dict[names[i]] = pic
    SAVE_DIR = 'FINAL_cats/'
    for cat in pic_dict:
        try:
            if pic_dict[cat].y_r_eye < pic_dict[cat].y_l_eye:
                pic_dict[cat].transform(flip=True)
            else:
                pic_dict[cat].transform(flip=False)
            pic_dict[cat].crop_w_border()
            pic_dict[cat].save_to_file(SAVE_DIR)
        except:
            print(pic_dict[cat].name)

if __name__ == '__main__':
    DATASET = 'cat_dataset/'
    folders = []
    #Cat pictures are divided into folders that begin with 'CAT'
    #so we will loop through each folder and perform the cleaning
    #algorithm on their contents one at a time.
    for file in os.listdir(DATASET):
        if file.startswith('CAT'):
            folders.append(file)
    
    for folder in folders:
        try:
            pic_directory = DATASET + folder + '/'
            run(pic_directory)
        except:
            print(folder)
            continue