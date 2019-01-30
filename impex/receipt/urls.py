from django.urls import path, include
from .views import imported # , index

urlpatterns = [
    # path('', index, name='index'),
    path('import', imported, name='imported_receipt'),
]