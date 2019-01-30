# Generated by Django 2.1.5 on 2019-01-28 17:48

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('receipt', '0010_auto_20190128_1849'),
    ]

    operations = [
        migrations.DeleteModel(
            name='RwbReceiptNotice',
        ),
        migrations.AddField(
            model_name='rwbreceiptcartime',
            name='memo',
            field=models.CharField(default='', max_length=50),
        ),
        migrations.AddField(
            model_name='rwbreceiptcartime',
            name='notice',
            field=models.IntegerField(default=0),
        ),
        migrations.AddField(
            model_name='rwbreceiptcartime',
            name='route',
            field=models.CharField(default='', max_length=50),
        ),
    ]