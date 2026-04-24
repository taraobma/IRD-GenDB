#!/usr/bin/env python3

from flask import Flask, request, render_template

from string import Template

app = Flask(__name__)

@app.route("/") # info screen; add "continue to open database" button
def home():
    return render_template('home.html')
    
@app.route("/home/variants") # the is the open database
def variants():
    return render_template('variants.html')
    
@app.route("/home/patients") # separate tab requiring login
def login():
    return render_template('patients.html')
            
if __name__ == '__main__':
    app.run(debug=True)