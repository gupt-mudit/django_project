from django.urls import path
from .views.user_views import user_view  # Import the view from user_views.py

urlpatterns = [
    path('users/', user_view, name='user_view'),
]
