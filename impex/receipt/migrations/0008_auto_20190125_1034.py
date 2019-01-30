# Generated by Django 2.1.5 on 2019-01-25 07:34

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('receipt', '0007_auto_20190124_1906'),
    ]

    operations = [
        migrations.CreateModel(
            name='RwbReceiptCarTime',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('key_number', models.CharField(max_length=255)),
                ('date_import', models.IntegerField(default=0)),
                ('date_export', models.IntegerField(default=0)),
                ('md5hash', models.CharField(default='', max_length=32)),
                ('rwbill_no', models.CharField(max_length=50)),
                ('car_no', models.CharField(max_length=10)),
                ('doc_date', models.CharField(max_length=50)),
                ('doc_time', models.CharField(max_length=5)),
                ('get_date', models.CharField(max_length=50)),
                ('get_time', models.CharField(max_length=5)),
            ],
            options={
                'abstract': False,
            },
        ),
        migrations.DeleteModel(
            name='RwbReceiptExt',
        ),
    ]
