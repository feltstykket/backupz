#! /usr/bin/python3

from django.db import models
from django.core.exceptions import ValidationError
from django import forms
import django.utils.text

from . import humanize

class BytesFieldForm(forms.CharField):
    def __init__(self, *args, **kwargs):
       #print('BytesFieldForm::__init__: %s :: %s' % (args, kwargs))
        super(BytesFieldForm, self).__init__(*args, **kwargs)
        self.validators=[humanize.dehumanize]


class BytesField(models.BigIntegerField, metaclass=models.SubfieldBase):
    description = 'Store bytes, but add/show in MB/GB/TB'

    def __init__(self, *args, **kwargs):
       #print('BytesField::__init__: %s :: %s' % (args, kwargs))
        super(BytesField, self).__init__(*args, **kwargs)


    def value_to_string(self, obj):
        value = self.value_from_object(obj)
        return self.get_prep_value(value)

    def formfield(self, **kwargs):
        defaults = {'required': not self.blank,
                    'label': django.utils.text.capfirst(self.verbose_name),
                    'help_text': self.help_text,
                    }
        if self.has_default():
            defaults['initial'] = self.get_default()
        defaults.update(kwargs)
        return BytesFieldForm(**defaults)

    def from_db_value(self, value, expression, connection, context):
        if value is None:
            return value

        #print('BytesField::from_db_value:', value)
        return humanize.humanize(value)


    def to_python(self, value):
        if isinstance(value, BytesField):
            return value

        if value is None:
            return value

        if isinstance(value, str):
            return value

        #print('BytesField::to_python: %s (%s)' % (value, humanize(value)))
        return humanize.humanize(value)


    def get_prep_value(self, value):
        if value is None:
            return value

        #print('BytesField::get_prep_value: %s' % value)
        return humanize.dehumanize(value)


    def get_db_prep_value(self, value, connection, prepared=False):
        if value is None:
            return value

       #print('BytesField::get_db_prep_value: %s' % value)
        return humanize.dehumanize(value)

    def clean(self, value, model_instance):
       #print('BytesField::clean: (%s)' % value)

        value = humanize.dehumanize(value)

        if value > 9223372036854775807:
            raise ValidationError('Max value is 9,223,372,036,854,775,807 bytes, or 7.9(ish) EB')

        return value

