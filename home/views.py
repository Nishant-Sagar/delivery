from django.shortcuts import render
from home.models import LocationUpdate
from django.http import JsonResponse


# Create your views here.
def index(request):
    return render(request, 'index.html')

def get_locations(request):
    locations = LocationUpdate.objects.latest('timestamp')
    return JsonResponse({
        'latitude': locations.latitude,
        'longitude': locations.longitude,
        'timestamp': locations.timestamp
    })
    