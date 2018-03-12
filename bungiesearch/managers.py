from django.conf import settings as dj_settings
from django.db.models import Manager

from . import Bungiesearch
from .logger import logger


class BungiesearchManager(Manager):
    """ A Django manager for integrated search into models. """
    model = None

    @property
    def search(self):
        index = Bungiesearch.get_index(self.model, via_class=True)
        return Bungiesearch().index(*index).doc_type(self.model.__name__)

    def search_index(self, index):
        if index not in Bungiesearch.get_index(self.model, via_class=True):
            logger.warning('Model/doctype {} is not present on index {}: search may return no results.'
                           .format(self.model.__name__, index))
        return Bungiesearch().index(index).doc_type(self.model.__name__)

    def custom_search(self, index, doc_type):
        """ Performs a search on a custom elasticsearch index and mapping. Will not attempt to map result objects. """
        return Bungiesearch(raw_results=True).index(index).doc_type(doc_type)

    def __getattr__(self, alias):
        """ Shortcut for search aliases.

            As explained in the docs (https://docs.python.org/2/reference/datamodel.html#object.__getattr__),
            this is only called as a last resort in case the attribute is not found.
            This function will check whether the given model is allowed to use
            the proposed alias and will raise an attribute error if not.
        """
        # Don't treat "private" attrs as possible aliases. This prevents an infinite recursion bug.
        # Similarly, if Bungiesearch is installed but not enabled, raise the expected error
        if alias[0] == '_' or not dj_settings.BUNGIESEARCH:
            raise AttributeError("'{}' object has no attribute '{}'".format(type(self), alias))

        return self.search.hook_alias(alias, self.model)
