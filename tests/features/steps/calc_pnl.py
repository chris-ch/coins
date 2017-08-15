import behave


@behave.given('following inflows and outflows')
def step_impl(context):
    pass


@behave.given('the reference currency is {reference_currency}')
def step_impl(context, reference_currency):
    raise NotImplementedError(u'STEP: Given the reference currency is {}'.format(reference_currency))


@behave.given('as of date is {as_of_date}')
def step_impl(context, as_of_date):
    raise NotImplementedError(u'STEP: Given as of date is {}'.format(as_of_date))


@behave.given('following trades are performed')
def step_impl(context):
    pass


@behave.given('forex rates are')
def step_impl(context):
    pass


@behave.then('P&L should be')
def step_impl(context):
    assert False
