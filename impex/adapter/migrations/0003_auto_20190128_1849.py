# Generated by Django 2.1.5 on 2019-01-28 15:49

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('adapter', '0002_auto_20190128_1604'),
    ]

    operations = [
        migrations.AlterField(
            model_name='rwbreceipt',
            name='key_number',
            field=models.CharField(max_length=255, unique=True),
        ),
        migrations.AlterField(
            model_name='rwbreceiptcar',
            name='key_number',
            field=models.CharField(max_length=255, unique=True),
        ),
    ]
