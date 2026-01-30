from celery import shared_task
from .models import Loan
from django.core.mail import send_mail
from django.conf import settings
from django.utils import timezone
from django.core.cache import cache

@shared_task
def send_loan_notification(loan_id):
    try:
        loan = Loan.objects.get(id=loan_id)
        member_email = loan.member.user.email
        book_title = loan.book.title
        send_mail(
            subject='Book Loaned Successfully',
            message=f'Hello {loan.member.user.username},\n\nYou have successfully loaned "{book_title}".\nPlease return it by the due date.',
            from_email=settings.DEFAULT_FROM_EMAIL,
            recipient_list=[member_email],
            fail_silently=False,
        )
    except Loan.DoesNotExist:
        pass


@shared_task(bind=True, max_retries=3, default_retry_delay=60, name="check_overdue_loans")
def check_overdue_loans(self) -> dict:

    """
    periodic task that finds overdue loans and send reminder emails

    - Overdue: is_returned = False and due_date < today
    - Idempotency: one email per loan per day using cache key
    - Retry: on temporary email failures, task is retried

    """


    today = timezone.now().date()
    loans = Loan.objects.filter(is_returned=False, due_date__lt=today).select_related("member__user", "book").iterator(chunk_size=50)


    processed = 0
    skipped = 0

    for loan in loans:
        user = loan.member.user
        cache_key = f"overdue_email:{loan.id}:{today.isoformat()}"


        if cache.get(cache_key):
            skipped += 1
            continue

        try:
            send_mail(
               
                subject=f"Overdue Notice: {loan.book.title}",
                message=(
                    f"Dear {user.first_name or user.username},Our records show that your loan for {loan.book.title} is overdue. Please return the book"
                ),
                from_email=settings.DEFAULT_FROM_EMAIL,
                recipient_list=[user.email],
                fail_silently=False,
               )

            cache.get(cache_key, True, timeout=60 * 60 * 24) # for 24 hours
            processed += 1

        except Exception as exc:

            print(f"[check_overdue_loan] error for loan {loan.id}: {exc}")

            try:
                raise self.retry(exc=exc)
            except self.MaxRetriesExceededError:
                continue

    
    return {
        "processed": processed,
        "skipped": skipped,
        "date": today.isoformat(),
    }