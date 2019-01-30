class DBRouter:
    """
    A router to control all database operations on models in applications.
    """
    @property
    def apps(self):
        return ['dicts', 'receipt', 'adapter']

    @property
    def default_base(self):
        return 'default'

    def db_for_read(self, model, **hints):
        """
        Attempts to read models go to app's db.
        """
        if model._meta.app_label not in self.apps:
            return self.default_base
        elif model._meta.app_label in self.apps:
            return model._meta.app_label
        else:
            return None

    def db_for_write(self, model, **hints):
        """
        Attempts to write models go to app's db.
        """
        if model._meta.app_label not in self.apps:
            return self.default_base
        elif model._meta.app_label in self.apps:
            return model._meta.app_label
        else:
            return None

    def allow_relation(self, obj1, obj2, **hints):
        """
        Allow relations if a model in the app is involved.
        """
        if obj1._state.db == self.default_base or obj2._state.db == self.default_base:
            return True
        elif obj1._state.db == obj2._state.db:
            return True
        else:
            return False

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        """
        Make sure the app's models only appears in the app's database.
        """
        if db == app_label and app_label in self.apps:
            return True
        if db == self.default_base and app_label not in self.apps:
            return True
        return False
