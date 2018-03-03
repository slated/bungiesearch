from distutils.version import StrictVersion

from django import get_version as get_django_version
from django.template import Context, loader
from django.template.defaultfilters import striptags
from django.utils.functional import cached_property
from six import iteritems, text_type, python_2_unicode_compatible

from elasticsearch_dsl.analysis import Analyzer
from elasticsearch_dsl.field import (
    Text, Keyword, Nested, Float, Double, Byte, Short, Integer, Long, Boolean, Date,
)
from elasticsearch_dsl.utils import AttrDict


@python_2_unicode_compatible
class AbstractField(object):
    """ Represents an elasticsearch index field and values from given objects.

    Currently does not support binary fields, but those can be created by manually providing a dictionary.
    Values are extracted using the `model_attr` or `eval_as` attribute.
    """
    meta_fields = ['_index', '_uid', '_type', '_id']
    common_fields = ['index_name', 'store', 'index', 'boost', 'null_value', 'copy_to', 'type', 'fields']

    _system_fields = ('base_field', 'base_field_class', 'eval_func', 'model_attr', 'template_name')

    @property
    def fields(self):
        if self.base_field_class is not None:
            return None
        raise NotImplementedError('Allowed fields are not defined.')

    @property
    def coretype(self):
        if self.base_field_class is not None:
            return self.base_field_class.name
        raise NotImplementedError('Core type is not defined!')

    @property
    def defaults(self):
        """ Stores default values. """
        return {}

    @cached_property
    def base_field(self):
        return self.get_base_field()

    @cached_property
    def base_field_class(self):
        return self.get_base_field_class()

    def get_base_field(self):
        # build base field if requested only.
        if self.base_field_class is not None:
            return self.base_field_class(**self._dsl_field_kwargs())
        return None

    def get_base_field_class(self):
        return None

    def __init__(self, **args):
        """
        Performs several checks to ensure that the provided attributes are valid. Will not check their values.
        """
        self.model_attr = args.pop('model_attr', None)
        self.eval_func = args.pop('eval_as', None)
        self.template_name = args.pop('template', None)

        if isinstance(self.coretype, list):
            if 'coretype' not in args:
                raise KeyError('{} can be represented as one of the following types: {}. '
                               'Specify which to select as the `coretype` parameter.'
                               .format(text_type(self), ', '.join(self.coretype)))
            if args['coretype'] not in self.coretype:
                raise KeyError('Core type {} is not supported by {}.'
                               .format(args['coretype'], text_type(self)))
            self.type = args.pop('coretype')
        else:
            self.type = self.coretype

        for attr, value in iteritems(args):
            if ((not self.base_field_class or self.fields is not None)
                    and attr not in self.fields
                    and attr not in AbstractField.common_fields):
                raise KeyError('Attribute `{}` is not allowed for core type {}.'
                               .format(attr, self.coretype))
            setattr(self, attr, value)

        for attr, value in iteritems(self.defaults):
            if not hasattr(self, attr):
                setattr(self, attr, value)

    def _dsl_field_kwargs(self, kwargs=None):
        field_kwargs = {}
        for attr, val in iteritems(self.__dict__):
            if attr in self._system_fields:
                continue
            elif (self.base_field_class is None
                  and attr in ('analyzer', 'index_analyzer', 'search_analyzer')
                  and isinstance(val, Analyzer)):
                # Serialize only if we don't have base field.
                # Otherwise let base_field do it for us.
                field_kwargs[attr] = val.to_dict()
                continue
            field_kwargs[attr] = val

        field_kwargs.update(kwargs or {})
        return field_kwargs

    def get_object_value(self, obj):
        if self.template_name:
            context = {'object': obj}
            if StrictVersion(get_django_version()) < StrictVersion('1.7'):
                context = Context(context)

            t = loader.select_template([self.template_name])
            return t.render(context)

        if self.eval_func:
            try:
                return eval(self.eval_func)
            except Exception as e:
                raise type(e)('Could not compute value of {} field (eval_as=`{}`): {}.'
                              .format(text_type(self), self.eval_func, text_type(e)))

        elif self.model_attr:
            if isinstance(obj, dict):
                return obj[self.model_attr]
            current_obj = getattr(obj, self.model_attr)

            if callable(current_obj):
                return current_obj()
            return current_obj

        else:
            raise KeyError('{0} gets its value via a model attribute, an eval function, '
                           'a template, or is prepared in a method call but none of '
                           '`model_attr`, `eval_as,` `template,` `prepare_{0}` is provided.'
                           .format(text_type(self)))

    def value(self, obj):
        """ Computes the value of this field to update the index.

        :param obj: object instance, as a dictionary or as a model instance.
        """
        value = self.get_object_value(obj)
        if self.base_field is not None:
            return self.base_field.serialize(value)
        return value

    def json(self):
        if self.base_field is not None:
            return self.base_field.to_dict()
        return self._dsl_field_kwargs()

    to_dict = json

    def __getattr__(self, item):
        # to be able to use bungiesearch fields as a part of Inner fields,
        # we should mimic elasticsearch_dsl fields behaviour.
        # will work only for fields with `base_field_class` not empty.
        if self.base_field_class is not None and hasattr(self.base_field_class, item):
            return getattr(self.base_field, item)
        return super(AbstractField, self).__getattribute__(item)

    def __str__(self):
        return self.__class__.__name__


# All the following definitions could probably be done with better polymorphism.
class TextField(AbstractField):
    coretype = 'text'
    fields = ['analyzer', 'boost', 'eager_global_ordinals', 'fielddata',
              'fielddata_frequency_filter', 'fields', 'index', 'index_options',
              'norms', 'position_increment_gap', 'store', 'search_analyzer',
              'search_quote_analyzer', 'similarity', 'term_vector']
    defaults = {'analyzer': 'snowball'}
    base_field_class = Text

    def value(self, obj):
        val = super(TextField, self).value(obj)
        if val is None:
            return None
        return striptags(val)


class KeywordField(AbstractField):
    coretype = 'keyword'
    fields = ['boost', 'doc_values', 'eager_global_ordinals', 'fields', 'ignore_above',
              'index', 'index_options', 'norms', 'null_value',
              'store', 'similarity', 'normalizer']
    base_field_class = Keyword

    def value(self, obj):
        val = super(KeywordField, self).value(obj)
        if val is None:
            return None
        return striptags(val)


class NumberField(AbstractField):
    coretype = ['float', 'double', 'byte', 'short', 'integer', 'long']
    fields = ['doc_values', 'precision_step', 'include_in_all', 'ignore_malformed', 'coerce']

    _base_fields_map = {
        'float': Float, 'double': Double, 'byte': Byte,
        'short': Short, 'integer': Integer, 'long': Long,
    }

    def get_base_field_class(self):
        return self._base_fields_map.get(self.type)


class DateField(AbstractField):
    coretype = 'date'
    fields = ['format', 'doc_values', 'precision_step', 'include_in_all', 'ignore_malformed']
    base_field_class = Date


class BooleanField(AbstractField):
    coretype = 'boolean'
    fields = []  # No specific fields.
    base_field_class = Boolean


class NestedField(AbstractField):
    base_field_class = Nested

    def get_base_field(self):
        properties = {key: field.base_field
                      for key, field in self.properties.items()}
        return self.base_field_class(**self._dsl_field_kwargs({'properties': properties}))

    def value(self, obj):
        multi = self.base_field._multi
        value = self.get_object_value(obj)
        if not multi:
            nested = self.nested_value(value)
        else:
            nested = [self.nested_value(item) for item in value]
        return self.base_field.serialize(nested)

    def nested_value(self, obj):
        value = {}
        for key, field in self.properties.items():
            value[key] = field.value(obj)
        return AttrDict(value)


def django_field_to_index(field, **attr):
    """ Returns the index field type that would likely be associated with each Django type. """
    dj_type = field.get_internal_type()

    if dj_type in ('DateField', 'DateTimeField'):
        return DateField(**attr)
    elif dj_type in ('BooleanField', 'NullBooleanField'):
        return BooleanField(**attr)
    elif dj_type in ('DecimalField', 'FloatField'):
        return NumberField(coretype='float', **attr)
    elif dj_type in ('PositiveSmallIntegerField', 'SmallIntegerField'):
        return NumberField(coretype='short', **attr)
    elif dj_type in ('IntegerField', 'PositiveIntegerField', 'AutoField'):
        return NumberField(coretype='integer', **attr)
    elif dj_type in ('BigIntegerField', ):
        return NumberField(coretype='long', **attr)

    return StringField(**attr)
