import base64
from io import BytesIO
from PIL import Image
from datetime import datetime
import pytz

import numpy as np
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
import plotly.graph_objects as go
import plotly.express as px

from keras.models import load_model, Model, Sequential
from keras.layers import Activation, Flatten, Reshape
from keras.layers.convolutional import Conv2DTranspose
from keras.layers.embeddings import Embedding
from keras import backend as K
K.set_image_data_format('channels_last')
import keras.backend.tensorflow_backend as tb
tb._SYMBOLIC_SCOPE.value = True

####################################################
#importing model and creating input values
####################################################
MODEL_FN = 'decoder_25c_not_random.h5'
BATCH_SIZE = 32
N_COMPONENTS = 25
N_IMAGES = 200

X = np.array(np.arange(N_IMAGES))
X_cats = np.array(np.arange(100))
X_humans = np.array(np.arange(100, 200))

####################################################
#splitting the model into encoder and decoder functions
####################################################
model = load_model(MODEL_FN)
decoder = K.function([model.get_layer('decoder_input').input, K.learning_phase()], model.layers[-1].output)
encoder = Model(inputs=model.input, outputs=model.get_layer('encoder_output').output)

####################################################
#extracting latent space vectors and computing means/std
####################################################
encoded_full = encoder.predict(X, batch_size=BATCH_SIZE)
encoded_cats = encoded_full[:100]
encoded_humans = encoded_full[100:]
component_mean = np.mean(encoded_full, axis=0)
cats_mean = np.mean(encoded_cats, axis=0)
humans_mean = np.mean(encoded_humans, axis=0)
component_std = np.std(encoded_full, axis=0)

####################################################
#below is where PCA was performed to make the latent space vectors
#linearly independent. this will be added back into the model
#once the accuracy is higher.
####################################################
#covariance = np.cov((encoded_full - component_mean).T)
#eigvals, eigvecs = np.linalg.eig(covariance)
#idx = np.argsort(-eigvals)
#eigvals = np.sqrt(eigvals[idx])
#eigvecs = eigvecs[:, idx]

####################################################
#defining constants and formatting for application interface
####################################################
FACE_WIDTH = '45%'
N_SLIDER_COLS = 1
N_SLIDERS = 25
COLUMN_WIDTH = '100%'
SLIDER_WIDTH = '90%'
SLIDER_MIN = -3.0
SLIDER_MAX = 3.0
INITIAL_VAL = 0
SLIDER_STEP = 0.01
colors = {'background': '#D0CCD0', 'text': '#420039'}
#font_family = 'Trebuchet MS'
font_family = 'Frutiger, Frutiger Linotype, Univers, Calibri, Gill Sans, Gill Sans MT, Myriad Pro, Myriad, DejaVu Sans Condensed, Liberation Sans, Nimbus Sans L, Tahoma, Geneva, Helvetica Neue, Helvetica, Arial, sans-serif'
FACE_STYLE = {'width':FACE_WIDTH, 'display':'inline-block', 'margin-left':'25%', 'margin-right':'25%', 'margin-top':10, 'justify-content':'center', 'align-items':'center'}
COLUMN_STYLE = {'width':COLUMN_WIDTH, 'display':'inline-block', 'margin':'auto', 'justify-content':'center', 'align-items':'center'}
SLIDER_STYLE = {'width':SLIDER_WIDTH, 'display':'inline-block', 'margin-left':'5%', 'margin-right':'3%', 'justify-content':'center', 'align-items':'center'}
TITLE_STYLE = {'font-family':font_family, 'color':colors['text'], 'text-align':'center', 'margin':'auto', 'justify-content':'center', 'align-items':'center'}
TEXT_STYLE = {'font-family':font_family, 'color':colors['text'], 'text-align':'left', 'margin':'auto'}
CONTROL_STYLE = {'display':'inline-block', 'font-family':font_family, 'color':colors['text'], 'text-align':'left', 'margin-left':'30%', 'justify-content':'center', 'align-items':'center'}
VECTOR_SLIDER_MARKS = {-3:{'label':''}, -2:{'label':''}, -1:{'label':''}, 0:{'label':''}, 
                        1:{'label':''}, 2:{'label':''}, 3:{'label':''}}
FACE_SLIDER_MARKS = {1:{'label':'Cats 1-100', 
                        'style':{'font-family':font_family, 
                                'color':colors['text'], 
                                'margin-left':0}}, 
                    25:{'label':''}, 50:{'label':''}, 75:{'label':''},
                    101:{'label':'Humans 101-200', 
                        'style':{'font-family':font_family, 
                                'color':colors['text'], 
                                'justify-content':'right'}},
                    125:{'label':''}, 150:{'label':''}, 175:{'label':''}, 200:{'label':''}}
PROJECT_DESCRIPTION = ['''Welcome to the human/cat hybrid face generator! The purpose of this app is to
                    generate unique faces with combined features from both human and cat faces.
                    These features were automatically learned by a neural network and may not have
                    any intuitive meaning.''', html.Br(), html.Br(),
                    '''On the left, you can toggle through various controls: Randomize for generating
                    a random face, Reset for setting all sliders to their original values, Cat 
                    Average to set sliders to the average values of just cat faces, Human Average
                    to set sliders to the average values of just human faces, and Select a Face to pull up one of
                    the 200 images from the original dataset. Images 1 through 100 are cat faces, and
                    101 through 200 are human faces.''', html.Br(), html.Br(),
                    '''To the right are 25 sliders that represent the 25 latent space vectors. Adjusting the
                    sliders will update the image to reflect the predicted face based on the new
                    values of the latent space vectors. Enjoy!''']

####################################################
#defining helper functions
####################################################
def numpy_to_b64(array):
    '''
    converts image from numpy array to binary image that can be
    embedded in HTML format
    '''
    pil = Image.fromarray(array)
    bytes = BytesIO()
    pil.save(bytes, format='png')
    b64_img = base64.b64encode(bytes.getvalue()).decode("utf-8")
    return b64_img

def get_relative_slider_positions(orig_values):
    '''
    defines component vectors in terms of standard deviations
    away from the mean
    '''
    slider_pos = (orig_values - component_mean) / component_std
    return list(slider_pos)

####################################################
#application layout and content
####################################################
app = dash.Dash()
app.config.suppress_callback_exceptions = True

app.layout = html.Div(style={'backgroundColor': colors['background']}, children=[
    html.H1(children='Face Generator', style=TITLE_STYLE),
    html.Div(html.P(children=PROJECT_DESCRIPTION, style=TEXT_STYLE)),
    (html.Div([
        html.Div([html.Div(children='Controls', style=TITLE_STYLE),
                html.Div(dcc.RadioItems(id='radio', options=[
                    {'label':'Randomize', 'value':'randomize'}, 
                    {'label':'Reset', 'value':'reset'},
                    {'label':'Cat Average', 'value':'cat_avg'},
                    {'label':'Human Average', 'value':'human_avg'},
                    {'label':'Select a Face:', 'value':'face'}], value='randomize',
                    labelStyle={'display':'block'}), style=CONTROL_STYLE),
            html.Div(dcc.Slider(id='face_picker', min=1, max=200, step=1, value=1, included=False, 
                    tooltip={'always_visible':True, 'placement':'bottom'}, marks=FACE_SLIDER_MARKS), 
                    style={'margin-bottom':30, 'margin-left':'5%', 'margin-right':'5%'}),
            html.Div(html.Button('Submit', id='submit'), style=TITLE_STYLE),
            html.Div(html.Img(id='face'), style=FACE_STYLE),
            ], style={'display':'inline-block', 'margin':'auto', 'width':FACE_WIDTH, 
                    'justify-content':'center', 'align-items':'center'}),
        html.Div(id='sliders', style={'width':'55%', 'display':'inline-block', 'margin':'auto', 
                                    'justify-content':'center', 'align-items':'center'})]
    )
    )
    ])

####################################################
#creating latent space sliders in loop
####################################################
@app.callback(Output('sliders', 'children'), [Input('sliders', 'id')])
def create_sliders(id):
    slider_col = [html.Div('Latent Space Vectors', style=TITLE_STYLE)]
    sliders = []
    for col in range(N_SLIDERS):
        slider_id = col + 1
        slider = html.Div(id='individual_sliders', children=dcc.Slider(id='component-{}'.format(slider_id), 
            min=SLIDER_MIN, max=SLIDER_MAX, step=SLIDER_STEP, value=INITIAL_VAL,
            included=False, updatemode='drag', tooltip={'always_visible':False, 'placement':'right'}, 
            marks=VECTOR_SLIDER_MARKS), style=SLIDER_STYLE)
        sliders.append(slider)
    slider_div = html.Div(id='slider_area', children=sliders, style=COLUMN_STYLE)
    slider_col.append(slider_div)
    return slider_col

####################################################
#creating Input and Output lists of all slider values so
#they can be used in callback functions
####################################################
input_sliders = []
output_sliders = []
slider_vals = {}
for i in range(N_COMPONENTS):
    slider_id = 'component-{}'.format(i+1)
    slider_vals[slider_id] = INITIAL_VAL
    input_sliders.append(Input(slider_id, 'value'))
    output_sliders.append(Output(slider_id, 'value'))

####################################################
#generating new face based on current values of sliders
####################################################
@app.callback((Output('face', 'src')), input_sliders)
def get_new_face(*args):
    ctx = dash.callback_context
    try:
        if ctx.triggered:
            triggered_id = ctx.triggered[0]['prop_id'].split('.')[0]
            triggered_val = ctx.triggered[0]['value']
            slider_vals[triggered_id] = triggered_val
    except:
        low_dim = np.expand_dims(component_mean, axis=0)
        default_face = decoder([low_dim, 1])[0]
        default_face = (default_face * 255.0).astype(np.uint8)
        figure = numpy_to_b64(default_face)
        src = 'data:image/png;base64,{}'.format(figure)
    #the line below would be run (instead of the one below it) if PCA were implemented
    #low_dim = component_mean + np.dot(eigvecs, (list(slider_vals.values()) * eigvals).T).T
    low_dim = component_mean + list(slider_vals.values()) * component_std
    low_dim = np.expand_dims(low_dim, axis=0)
    pred_face = decoder([low_dim, 1])[0]
    new_face = (pred_face * 255.0).astype(np.uint8)
    figure = numpy_to_b64(new_face)
    src = 'data:image/png;base64,{}'.format(figure)
    return src

####################################################
#callback function for the control panel
####################################################
@app.callback(output_sliders, [Input('submit', 'n_clicks')], [State('radio', 'value'), State('face_picker', 'value')])
def random_reset(n_clicks, radio, face_picker):
    if n_clicks > 0:
        if radio == 'randomize':
            return list(np.clip(np.random.randn(N_COMPONENTS), -3.0, 3.0))
        elif radio == 'reset':
            return [0]*N_COMPONENTS
        elif radio == 'cat_avg':
            return get_relative_slider_positions(cats_mean)
        elif radio == 'human_avg':
            return get_relative_slider_positions(humans_mean)
        elif radio == 'face':
            face_id = face_picker - 1
            return get_relative_slider_positions(encoded_full[face_id])
        
if __name__ == '__main__':
    app.run_server(debug=True)
