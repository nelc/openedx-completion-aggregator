"""
Transformers for completion aggregation.
"""
from event_routing_backends.processors.openedx_filters.decorators import openedx_filter
from event_routing_backends.processors.xapi import constants
from event_routing_backends.processors.xapi.registry import XApiTransformersRegistry
from event_routing_backends.processors.xapi.transformer import XApiTransformer
from tincan import Activity, ActivityDefinition, LanguageMap, Result, Verb

from django.utils.functional import cached_property

XAPI_ACTIVITY_LESSON = "http://adlnet.gov/expapi/activities/lesson"


class BaseCompletionTransformer(XApiTransformer):
    """
    Base transformer for completion events.
    """

    _verb = Verb(
        id=constants.XAPI_VERB_COMPLETED,
        display=LanguageMap({constants.EN: constants.COMPLETED}),
    )
    object_type = None
    object_id = None

    @openedx_filter(
        filter_type="completion_aggregator.xapi.completion.get_object",
    )
    def get_object(self):
        """
        Get object for xAPI transformed event.

        Returns
        -------
            `Activity`

        """
        if not self.object_type or not self.object_id:
            raise NotImplementedError()  # pragma: no cover

        return Activity(
            id=self.object_id,
            definition=ActivityDefinition(
                type=self.object_type,
            ),
        )


@XApiTransformersRegistry.register("openedx.completion_aggregator.completion.chapter")
@XApiTransformersRegistry.register("openedx.completion_aggregator.completion.sequential")
class ModuleCompletionTransformer(BaseCompletionTransformer):
    """
    Transformer for events generated when a user completes a section or subsection.
    """

    object_type = constants.XAPI_ACTIVITY_MODULE

    @cached_property
    def object_id(self):
        """Returns the object identifier for the module completion transformer."""
        return super().get_object_iri("xblock", self.get_data("data.block_id", required=True))


@XApiTransformersRegistry.register("openedx.completion_aggregator.completion.vertical")
class LessonCompletionTransformer(ModuleCompletionTransformer):
    """
    Transformer for events generated when a user completes an unit.
    """

    object_type = getattr(constants, "XAPI_ACTIVITY_LESSON", XAPI_ACTIVITY_LESSON)


@XApiTransformersRegistry.register("openedx.completion_aggregator.completion.course")
class CourseCompletionTransformer(BaseCompletionTransformer):
    """
    Transformer for event generated when a user completes a course.
    """

    object_type = constants.XAPI_ACTIVITY_COURSE

    @cached_property
    def object_id(self):
        """Returns the object identifier for the course completion transformer."""
        return super().get_object_iri("courses", self.get_data("data.course_id", required=True))

    def get_context_activities(self):
        """
        Retunrs context activities property.

        The XApiTransformer class implements this method and returns in the parent key
        an activity that contains the course metadata however this is not necessary in
        cases where a transformer uses the course metadata as object since the data is
        redundant and a course cannot be its own parent, therefore this must return None.

        Returns
        -------
            None

        """
        return None


class BaseProgressTransformer(XApiTransformer):
    """
    Base transformer for completion aggregator progress events.
    """

    _verb = Verb(
        id=constants.XAPI_VERB_PROGRESSED,
        display=LanguageMap({constants.EN: constants.PROGRESSED}),
    )
    object_type = None
    additional_fields = ('result', )

    @openedx_filter(
        filter_type="completion_aggregator.xapi.progress.get_object",
    )
    def get_object(self) -> Activity:
        """
        Get object for xAPI transformed event.
        """
        if not self.object_type:
            raise NotImplementedError()  # pragma: no cover

        return Activity(
            id=self.object_id,
            definition=ActivityDefinition(
                type=self.object_type,
            ),
        )

    def get_result(self) -> Result:
        """
        Get result for xAPI transformed event.
        """
        progress = self.get_data("data.percent") or 0
        return Result(
            completion=progress == 1.0,
            score={
                "scaled": self.get_data("data.percent") or 0
            }
        )


@XApiTransformersRegistry.register("openedx.completion_aggregator.progress.chapter")
@XApiTransformersRegistry.register("openedx.completion_aggregator.progress.sequential")
class ModuleProgressTransformer(BaseProgressTransformer):
    """
    Transformer for event generated when a user makes progress in a section or subsection.
    """

    object_type = constants.XAPI_ACTIVITY_MODULE

    @cached_property
    def object_id(self):
        """Returns the object identifier for the module progress transformer."""
        return super().get_object_iri("xblock", self.get_data("data.block_id"))


@XApiTransformersRegistry.register("openedx.completion_aggregator.progress.vertical")
class LessonProgressTransformer(ModuleProgressTransformer):
    """
    Transformer for event generated when a user makes progress in an unit.
    """

    object_type = getattr(constants, "XAPI_ACTIVITY_LESSON", XAPI_ACTIVITY_LESSON)


@XApiTransformersRegistry.register("openedx.completion_aggregator.progress.course")
class CourseProgressTransformer(BaseProgressTransformer):
    """
    Transformer for event generated when a user makes progress in a course.
    """

    object_type = constants.XAPI_ACTIVITY_COURSE

    @cached_property
    def object_id(self):
        """Returns the object identifier for the course progress transformer."""
        return super().get_object_iri("courses", self.get_data("data.course_id"))

    def get_context_activities(self):
        """
        Retunrs context activities property.

        The XApiTransformer class implements this method and returns in the parent key
        an activity that contains the course metadata however this is not necessary in
        cases where a transformer uses the course metadata as object since the data is
        redundant and a course cannot be its own parent, therefore this must return None.

        Returns
        -------
            None

        """
        return None
