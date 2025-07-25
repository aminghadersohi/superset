/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import { useEffect, useMemo, useState } from 'react';
import { SupersetClient, t } from '@superset-ui/core';
import { useFavoriteStatus, useListViewResource } from 'src/views/CRUD/hooks';
import { Dashboard, DashboardTableProps, TableTab } from 'src/views/CRUD/types';
import handleResourceExport from 'src/utils/export';
import { useHistory } from 'react-router-dom';
import {
  getItem,
  LocalStorageKeys,
  setItem,
} from 'src/utils/localStorageHelpers';
import { LoadingCards } from 'src/pages/Home';
import {
  CardContainer,
  createErrorHandler,
  getFilterValues,
  PAGE_SIZE,
  handleDashboardDelete,
} from 'src/views/CRUD/utils';
import withToasts from 'src/components/MessageToasts/withToasts';
import { DeleteModal, Loading } from '@superset-ui/core/components';
import PropertiesModal from 'src/dashboard/components/PropertiesModal';
import DashboardCard from 'src/features/dashboards/DashboardCard';
import { Icons } from '@superset-ui/core/components/Icons';
import { navigateTo } from 'src/utils/navigationUtils';
import EmptyState from './EmptyState';
import SubMenu from './SubMenu';
import { WelcomeTable } from './types';

function DashboardTable({
  user,
  addDangerToast,
  addSuccessToast,
  mine,
  showThumbnails,
  otherTabData,
  otherTabFilters,
  otherTabTitle,
}: DashboardTableProps) {
  const history = useHistory();
  const defaultTab = getItem(
    LocalStorageKeys.HomepageDashboardFilter,
    TableTab.Other,
  );

  const filteredOtherTabData = otherTabData.filter(obj => !('viz_type' in obj));

  const {
    state: { loading, resourceCollection: dashboards },
    setResourceCollection: setDashboards,
    hasPerm,
    refreshData,
    fetchData,
  } = useListViewResource<Dashboard>(
    'dashboard',
    t('dashboard'),
    addDangerToast,
    true,
    defaultTab === TableTab.Mine ? mine : filteredOtherTabData,
    [],
    false,
  );
  const dashboardIds = useMemo(() => dashboards.map(c => c.id), [dashboards]);
  const [saveFavoriteStatus, favoriteStatus] = useFavoriteStatus(
    'dashboard',
    dashboardIds,
    addDangerToast,
  );

  const [editModal, setEditModal] = useState<Dashboard>();
  const [activeTab, setActiveTab] = useState(defaultTab);
  const [preparingExport, setPreparingExport] = useState<boolean>(false);
  const [loaded, setLoaded] = useState<boolean>(false);
  const [dashboardToDelete, setDashboardToDelete] = useState<Dashboard | null>(
    null,
  );

  const getData = (tab: TableTab) =>
    fetchData({
      pageIndex: 0,
      pageSize: PAGE_SIZE,
      sortBy: [
        {
          id: 'changed_on_delta_humanized',
          desc: true,
        },
      ],
      filters: getFilterValues(
        tab,
        WelcomeTable.Dashboards,
        user,
        otherTabFilters,
      ),
    });

  useEffect(() => {
    if (loaded || activeTab === TableTab.Favorite) {
      getData(activeTab);
    }
    setLoaded(true);
  }, [activeTab]);

  const handleBulkDashboardExport = (dashboardsToExport: Dashboard[]) => {
    const ids = dashboardsToExport.map(({ id }) => id);
    handleResourceExport('dashboard', ids, () => {
      setPreparingExport(false);
    });
    setPreparingExport(true);
  };

  const handleDashboardEdit = (edits: Dashboard) =>
    SupersetClient.get({
      endpoint: `/api/v1/dashboard/${edits.id}`,
    }).then(
      ({ json = {} }) => {
        setDashboards(
          dashboards.map(dashboard => {
            if (dashboard.id === json.id) {
              return json.result;
            }
            return dashboard;
          }),
        );
      },
      createErrorHandler(errMsg =>
        addDangerToast(
          t('An error occurred while fetching dashboards: %s', errMsg),
        ),
      ),
    );

  const menuTabs = [
    {
      name: TableTab.Favorite,
      label: t('Favorite'),
      onClick: () => {
        setActiveTab(TableTab.Favorite);
        setItem(LocalStorageKeys.HomepageDashboardFilter, TableTab.Favorite);
      },
    },
    {
      name: TableTab.Mine,
      label: t('Mine'),
      onClick: () => {
        setActiveTab(TableTab.Mine);
        setItem(LocalStorageKeys.HomepageDashboardFilter, TableTab.Mine);
      },
    },
  ];

  if (otherTabData) {
    menuTabs.push({
      name: TableTab.Other,
      label: otherTabTitle,
      onClick: () => {
        setActiveTab(TableTab.Other);
        setItem(LocalStorageKeys.HomepageDashboardFilter, TableTab.Other);
      },
    });
  }

  if (loading) return <LoadingCards cover={showThumbnails} />;
  return (
    <>
      <SubMenu
        activeChild={activeTab}
        backgroundColor="transparent"
        tabs={menuTabs}
        buttons={[
          {
            icon: (
              <Icons.PlusOutlined
                iconSize="m"
                data-test="add-annotation-layer-button"
              />
            ),
            name: t('Dashboard'),
            buttonStyle: 'secondary',
            onClick: () => {
              navigateTo('/dashboard/new', { assign: true });
            },
          },
          {
            name: t('View All »'),
            buttonStyle: 'link',
            onClick: () => {
              const target =
                activeTab === TableTab.Favorite
                  ? `/dashboard/list/?filters=(favorite:(label:${t(
                      'Yes',
                    )},value:!t))`
                  : '/dashboard/list/';
              history.push(target);
            },
          },
        ]}
      />
      {editModal && (
        <PropertiesModal
          dashboardId={editModal?.id}
          show
          onHide={() => setEditModal(undefined)}
          onSubmit={handleDashboardEdit}
        />
      )}
      {dashboardToDelete && (
        <DeleteModal
          description={
            <>
              {t('Are you sure you want to delete')}{' '}
              <b>{dashboardToDelete.dashboard_title}</b>?
            </>
          }
          onConfirm={() => {
            handleDashboardDelete(
              dashboardToDelete,
              refreshData,
              addSuccessToast,
              addDangerToast,
              activeTab,
              user?.userId,
            );
            setDashboardToDelete(null);
          }}
          onHide={() => setDashboardToDelete(null)}
          open={!!dashboardToDelete}
          title={t('Please confirm')}
        />
      )}
      {dashboards.length > 0 && (
        <CardContainer showThumbnails={showThumbnails}>
          {dashboards.map(e => (
            <DashboardCard
              key={e.id}
              dashboard={e}
              hasPerm={hasPerm}
              bulkSelectEnabled={false}
              showThumbnails={showThumbnails}
              userId={user?.userId}
              loading={loading}
              openDashboardEditModal={(dashboard: Dashboard) =>
                setEditModal(dashboard)
              }
              saveFavoriteStatus={saveFavoriteStatus}
              favoriteStatus={favoriteStatus[e.id]}
              handleBulkDashboardExport={handleBulkDashboardExport}
              onDelete={dashboard => setDashboardToDelete(dashboard)}
            />
          ))}
        </CardContainer>
      )}
      {dashboards.length === 0 && (
        <EmptyState tableName={WelcomeTable.Dashboards} tab={activeTab} />
      )}
      {preparingExport && <Loading />}
    </>
  );
}

export default withToasts(DashboardTable);
