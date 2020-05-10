class Step:

    def __init__(self, name, params, stage):
        self.name = name
        self.params = params
        self.stage = stage

    def invalid_step(self, stage):
        raise AttributeError("Invalid step: " + self.name)

    def step_fit(self, stage):
        return stage.fit(**self.params)

    def step_transform(self, stage):
        return stage.transform(**self.params)

    def step_predict(self, stage):
        return stage.predict(**self.params)

    def step_load(self, stage):
        return stage.load(**self.params)

    def step_save(self, stage):
        if 'overwrite' in self.params and self.params['overwrite'] == 'true':
            return stage.write().overwrite().save(self.params['path'])
        else:
            return stage.write().save(self.params['path'])

    def execute(self):
        switcher = {
            'fit': self.step_fit,
            'transform': self.step_transform,
            'predict': self.step_predict,
            'load': self.step_load,
            'save': self.step_save
        }
        step_func = switcher.get(self.name, self.invalid_step)
        return step_func(self.stage)
