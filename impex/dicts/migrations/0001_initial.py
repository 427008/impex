# Generated by Django 2.1.5 on 2019-01-17 14:49

from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='CargoType',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('key_number', models.CharField(max_length=255)),
                ('name', models.CharField(max_length=255)),
                ('name_short', models.CharField(max_length=255)),
                ('name_lat', models.CharField(max_length=255)),
                ('name_lat_short', models.CharField(max_length=255)),
                ('date_import', models.IntegerField(default=0)),
                ('date_export', models.IntegerField(default=0)),
            ],
            options={
                'abstract': False,
            },
        ),
        migrations.CreateModel(
            name='Contract',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('key_number', models.CharField(max_length=255)),
                ('name', models.CharField(max_length=255)),
                ('name_short', models.CharField(max_length=255)),
                ('name_lat', models.CharField(max_length=255)),
                ('name_lat_short', models.CharField(max_length=255)),
                ('date_import', models.IntegerField(default=0)),
                ('date_export', models.IntegerField(default=0)),
            ],
            options={
                'abstract': False,
            },
        ),
        migrations.CreateModel(
            name='LegalPerson',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('key_number', models.CharField(max_length=255)),
                ('name', models.CharField(max_length=255)),
                ('name_short', models.CharField(max_length=255)),
                ('name_lat', models.CharField(max_length=255)),
                ('name_lat_short', models.CharField(max_length=255)),
                ('date_import', models.IntegerField(default=0)),
                ('date_export', models.IntegerField(default=0)),
            ],
            options={
                'abstract': False,
            },
        ),
        migrations.CreateModel(
            name='Stations',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('key_number', models.CharField(max_length=255)),
                ('name', models.CharField(max_length=255)),
                ('name_short', models.CharField(max_length=255)),
                ('name_lat', models.CharField(max_length=255)),
                ('name_lat_short', models.CharField(max_length=255)),
                ('date_import', models.IntegerField(default=0)),
                ('date_export', models.IntegerField(default=0)),
            ],
            options={
                'abstract': False,
            },
        ),
        migrations.CreateModel(
            name='Warehouse',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('key_number', models.CharField(max_length=255)),
                ('name', models.CharField(max_length=255)),
                ('name_short', models.CharField(max_length=255)),
                ('name_lat', models.CharField(max_length=255)),
                ('name_lat_short', models.CharField(max_length=255)),
                ('date_import', models.IntegerField(default=0)),
                ('date_export', models.IntegerField(default=0)),
            ],
            options={
                'abstract': False,
            },
        ),
    ]
