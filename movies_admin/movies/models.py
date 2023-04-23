from django.db import models
from django.core.validators import MinValueValidator, MaxValueValidator
from django.utils.translation import gettext_lazy as _
import uuid


class TimeStampedMixin(models.Model):
    created = models.DateTimeField(auto_now_add=True)
    modified = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True


class UUIDMixin(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    class Meta:
        abstract = True


class FilmWork(UUIDMixin, TimeStampedMixin):

    class Type(models.TextChoices):
        MOVIE = "MV", _("Movie")
        NOT_MOVIE = "NM", _("Not_movie")

    title = models.CharField(_("title"), max_length=255)
    description = models.TextField(_("description"), blank=True)
    creation_date = models.DateTimeField(_("creation_date"))
    rating = models.FloatField(_("rating"), blank=True,
                               validators=[MinValueValidator(0),
                                           MaxValueValidator(100)])
    type = models.TextField(_("type"), choices=Type.choices)
    genres = models.ManyToManyField("Genre", through="GenreFilmWork")
    persons = models.ManyToManyField("Person", through="PersonFilmWork")

    def __str__(self):
        return self.title

    class Meta:
        db_table = "content\".\"film_work"
        verbose_name = _("Film")
        verbose_name_plural = _("Films")
        indexes = [models.Index(fields=["creation_date", "rating"])]


class Person(UUIDMixin, TimeStampedMixin):
    full_name = models.TextField(_("full_name"), null=False)

    def __str__(self):
        return self.full_name

    class Meta:
        db_table = "content\".\"person"
        verbose_name = _("Person")
        verbose_name_plural = _("Persons")


class Genre(UUIDMixin, TimeStampedMixin):
    name = models.CharField(_("name"), max_length=255)
    description = models.TextField(_("description"), blank=True)

    def __str__(self):
        return self.name

    class Meta:
        db_table = "content\".\"genre"
        verbose_name = _("Genre")
        verbose_name_plural = _("Genres")


class GenreFilmWork(UUIDMixin):
    film_work = models.ForeignKey("FilmWork", on_delete=models.CASCADE)
    genre = models.ForeignKey("Genre", on_delete=models.CASCADE)
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "content\".\"genre_film_work"
        constraints = [models.UniqueConstraint(fields=["film_work", "genre"], name="film_work_genre_idx")]
        indexes = [models.Index(fields=["film_work", "genre"])]


class PersonFilmWork(UUIDMixin):

    class Role(models.TextChoices):
        ACTOR = "AC", _("Actor")
        WRITER = "WR", _("Writer")
        DIRECTOR = "DR", _("Director")

    film_work = models.ForeignKey("FilmWork", on_delete=models.CASCADE)
    person = models.ForeignKey("Person", on_delete=models.CASCADE)
    role = models.TextField(_("role"), choices=Role.choices, null=True)
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "content\".\"person_film_work"
        constraints = [models.UniqueConstraint(fields=["film_work", "person"], name="film_work_person_idx"),
                       models.UniqueConstraint(fields=["film_work", "person", "role"], name="film_work_person_role")]
        indexes = [models.Index(fields=["film_work", "person"]),
                   models.Index(fields=["film_work", "person", "role"])]
