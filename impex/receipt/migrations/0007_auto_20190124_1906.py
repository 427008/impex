# Generated by Django 2.1.5 on 2019-01-24 16:06

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('receipt', '0006_rwbreceiptnotice_rwbreceiptnoticetime'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='rwbreceipt',
            name='route',
        ),
        migrations.AlterField(
            model_name='rwbreceiptnotice',
            name='notice',
            field=models.IntegerField(default=0),
        ),
        migrations.AlterField(
            model_name='rwbreceiptnoticetime',
            name='notice',
            field=models.IntegerField(default=0),
        ),
        migrations.AlterField(
            model_name='rwbreceiptnoticetime',
            name='path_no',
            field=models.IntegerField(default=0),
        ),
    ]