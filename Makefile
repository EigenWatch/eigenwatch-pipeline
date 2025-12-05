# ------------------------------------------------
# Alembic config paths
# ------------------------------------------------
EVENTS_ALEMBIC = alembic/events/alembic.ini
ANALYTICS_ALEMBIC = alembic/analytics/alembic.ini

# ------------------------------------------------
# EVENTS – Migrations
# ------------------------------------------------

events-current:
	alembic -c $(EVENTS_ALEMBIC) current

events-history:
	alembic -c $(EVENTS_ALEMBIC) history

events-revision:
	alembic -c $(EVENTS_ALEMBIC) revision -m "$(m)"

events-autogen:
	alembic -c $(EVENTS_ALEMBIC) revision --autogenerate -m "$(m)"

events-upgrade:
	alembic -c $(EVENTS_ALEMBIC) upgrade $(t)

events-downgrade:
	alembic -c $(EVENTS_ALEMBIC) downgrade $(t)

events-stamp:
	alembic -c $(EVENTS_ALEMBIC) stamp $(t)


# ------------------------------------------------
# ANALYTICS – Migrations
# ------------------------------------------------

analytics-current:
	alembic -c $(ANALYTICS_ALEMBIC) current

analytics-history:
	alembic -c $(ANALYTICS_ALEMBIC) history

analytics-revision:
	alembic -c $(ANALYTICS_ALEMBIC) revision -m "$(m)"

analytics-autogen:
	alembic -c $(ANALYTICS_ALEMBIC) revision --autogenerate -m "$(m)"

analytics-upgrade:
	alembic -c $(ANALYTICS_ALEMBIC) upgrade $(t)

analytics-downgrade:
	alembic -c $(ANALYTICS_ALEMBIC) downgrade $(t)

analytics-stamp:
	alembic -c $(ANALYTICS_ALEMBIC) stamp $(t)


# ------------------------------------------------
# Helpers
# ------------------------------------------------

help:
	@echo ""
	@echo "Alembic Migration Commands"
	@echo "---------------------------"
	@echo ""
	@echo "EVENTS:"
	@echo "  make events-current"
	@echo "  make events-history"
	@echo "  make events-revision m=\"message\""
	@echo "  make events-autogen m=\"message\""
	@echo "  make events-upgrade t=\"head\""
	@echo "  make events-downgrade t=\"-1\""
	@echo "  make events-stamp t=\"head\""
	@echo ""
	@echo "ANALYTICS:"
	@echo "  make analytics-current"
	@echo "  make analytics-history"
	@echo "  make analytics-revision m=\"message\""
	@echo "  make analytics-autogen m=\"message\""
	@echo "  make analytics-upgrade t=\"head\""
	@echo "  make analytics-downgrade t=\"-1\""
	@echo "  make analytics-stamp t=\"head\""
	@echo ""
