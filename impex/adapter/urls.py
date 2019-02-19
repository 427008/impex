from django.urls import path, include
from .views import imported, imported_old #, index

urlpatterns = [
    # path('', index, name='index'),
    path('create', imported, name='adapter_create'),
    path('create/old', imported_old, name='adapter_create_old'),
]