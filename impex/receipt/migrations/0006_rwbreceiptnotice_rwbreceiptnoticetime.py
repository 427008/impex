# Generated by Django 2.1.5 on 2019-01-24 15:00

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('receipt', '0005_auto_20190124_1703'),
    ]

    operations = [
        migrations.CreateModel(
            name='RwbReceiptNotice',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('key_number', models.CharField(max_length=255)),
                ('date_import', models.IntegerField(default=0)),
                ('date_export', models.IntegerField(default=0)),
                ('md5hash', models.CharField(default='', max_length=32)),
                ('rwbill_no', models.CharField(max_length=50)),
                ('car_no', models.CharField(max_length=10)),
                ('route', models.CharField(max_length=50)),
                ('memo', models.CharField(max_length=50)),
                ('notice', models.CharField(max_length=50)),
            ],
            options={
                'abstract': False,
            },
        ),
        migrations.CreateModel(
            name='RwbReceiptNoticeTime',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('key_number', models.CharField(max_length=255)),
                ('date_import', models.IntegerField(default=0)),
                ('date_export', models.IntegerField(default=0)),
                ('md5hash', models.CharField(default='', max_length=32)),
                ('route', models.CharField(max_length=50)),
                ('memo', models.CharField(max_length=50)),
                ('notice', models.CharField(max_length=50)),
                ('path_no', models.CharField(max_length=50)),
                ('in_date', models.CharField(max_length=50)),
                ('in_time', models.CharField(max_length=5)),
                ('end_date', models.CharField(max_length=50)),
                ('end_time', models.CharField(max_length=5)),
            ],
            options={
                'abstract': False,
            },
        ),
    ]
