from django import forms


class LoginForm(forms.Form):
    password = forms.CharField()
