from datetime import timedelta
from rest_framework import viewsets, status
from rest_framework.response import Response
from .models import Author, Book, Member, Loan
from .serializers import AuthorSerializer, BookSerializer, MemberSerializer, LoanSerializer,TopActiveMemberSerializer
from rest_framework.decorators import action
from django.utils import timezone
from .tasks import send_loan_notification
from django.db.models import Q, Count

class AuthorViewSet(viewsets.ModelViewSet):
    queryset = Author.objects.all()
    serializer_class = AuthorSerializer

class BookViewSet(viewsets.ModelViewSet):
    queryset = Book.objects.all()
    serializer_class = BookSerializer

    @action(detail=True, methods=['post'])
    def loan(self, request, pk=None):
        book = self.get_object()
        if book.available_copies < 1:
            return Response({'error': 'No available copies.'}, status=status.HTTP_400_BAD_REQUEST)
        member_id = request.data.get('member_id')
        try:
            member = Member.objects.get(id=member_id)
        except Member.DoesNotExist:
            return Response({'error': 'Member does not exist.'}, status=status.HTTP_400_BAD_REQUEST)
        loan = Loan.objects.create(book=book, member=member)
        book.available_copies -= 1
        book.save()
        send_loan_notification.delay(loan.id)
        return Response({'status': 'Book loaned successfully.'}, status=status.HTTP_201_CREATED)

    @action(detail=True, methods=['post'])
    def return_book(self, request, pk=None):
        book = self.get_object()
        member_id = request.data.get('member_id')
        try:
            loan = Loan.objects.get(book=book, member__id=member_id, is_returned=False)
        except Loan.DoesNotExist:
            return Response({'error': 'Active loan does not exist.'}, status=status.HTTP_400_BAD_REQUEST)
        loan.is_returned = True
        loan.return_date = timezone.now().date()
        loan.save()
        book.available_copies += 1
        book.save()
        return Response({'status': 'Book returned successfully.'}, status=status.HTTP_200_OK)

class MemberViewSet(viewsets.ModelViewSet):
    queryset = Member.objects.all()
    serializer_class = MemberSerializer

    @action(detail=False, methods=["get"], url_path="top-active")
    def top_active(self, request):
        members = Member.objects.annotate(
            active_loans=Count("loans", filter=Q(loans__is_returned=False))
        ).filter(active_loans__gt=0).order_by("-active_loans")
        serializer = TopActiveMemberSerializer(members, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)

class LoanViewSet(viewsets.ModelViewSet):
    queryset = Loan.objects.all()
    serializer_class = LoanSerializer


    @action(detail=True, methods=["post"])
    def extend_due_date(self, request, pk=None):
        """Extend the due date of a loan by a given number of days."""
        loan = self.get_object()
        raw_additional_days = request.data.get("additional_days")

        try:
            additional_days = int(raw_additional_days)
            if additional_days <= 0:
                raise ValueError
        except (TypeError, ValueError):
            return Response(
                {"error": "additional_days must be a positive integer."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        if loan.due_date and loan.due_date < timezone.now().date():
            return Response(
                {"error": "Due date is already overdue."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        if not loan.due_date:
            loan.due_date = loan.loan_date

        loan.due_date += timedelta(days=additional_days)
        loan.save(update_fields=["due_date"])

        serializer = LoanSerializer(loan)
        return Response(serializer.data, status=status.HTTP_200_OK)