from django.urls import path, include
from .views import imported # , index

urlpatterns = [
    # path('', index, name='index'),
    path('create', imported, name='adapter_create'),
]